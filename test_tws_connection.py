import os
import json
import requests
import sys
import urllib3
from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================================================================
# CONFIGURATION
# TWS_HOST and TWS_ENGINE confirmed from your screenshots
# Only update TWS_PASSWORD below
# ================================================================
TWS_HOST     = "https://twsus-prod.barcapint.com:9443"
TWS_ENGINE   = "TWSCF_PROD"       # confirmed: Engine = TWSCF_PROD [twsmaster]
TWS_USER     = "x01532741"        # confirmed: your login username
TWS_PASSWORD = "your_tws_password" # <-- replace with your real password

# One real job from your Hive table
TEST_STREAM = "RFTBDHING01DAY1"
TEST_JOB    = "RFT-BDH-INGEST-CR-BURAU-PRMR-ATRB-TRU-HS"

SEP = "=" * 65

# ================================================================
# All known IBM Dynamic Workload Console REST API endpoint patterns
# Based on: /console/login.jsp, Engine: TWSCF_PROD [twsmaster]
# ================================================================
CANDIDATE_ENDPOINTS = [
    "/JobManagerRESTWeb/JobStream/Library",
    "/JobManagerRESTWeb/jobs",
    "/JobManagerRESTWeb/run/job",
    "/JobManagerRESTWeb/plan/jobs",
    "/JobManagerRESTWeb/plan/jobstreams",
    "/JobManagerRESTWeb/monitoring/jobs",
    "/JobManagerRESTWeb/monitoring/jobstreams",
    f"/JobManagerRESTWeb/{TWS_ENGINE}/plan/jobs",
    f"/JobManagerRESTWeb/{TWS_ENGINE}/monitoring/jobs",
    "/twsWaRESTWeb/rest/batchJobs",
    "/twsWaRESTWeb/rest/jobstreams",
    "/twsWaRESTWeb/rest/runs",
    "/twsWaRESTWeb/rest/plan/jobs",
    "/console/rest/batchJobs",
    "/console/rest/jobs",
    "/console/rest/jobstreams",
    "/console/rest/plan/jobs",
    "/wa/rest/batchJobs",
    "/wa/rest/jobstreams",
    "/wa/rest/plan/jobs",
    "/twsd/api/v1/batchjobs",
    "/twsd/api/v2/batchjobs",
    "/twsd/api/v1/plan/jobs",
]


# ================================================================
# STEP 1 — CREDENTIAL CHECK
# ================================================================
def check_credentials():
    print(SEP)
    print("CREDENTIAL CHECK")
    print(SEP)
    if TWS_PASSWORD == "your_tws_password":
        print("FAIL  TWS_PASSWORD is still the placeholder.")
        print("      Open this file and set your real password:")
        print("        TWS_PASSWORD = 'your_actual_password'")
        sys.exit(1)
    print(f"OK    TWS_HOST     = {TWS_HOST}")
    print(f"OK    TWS_ENGINE   = {TWS_ENGINE}")
    print(f"OK    TWS_USER     = {TWS_USER}")
    print(f"OK    TWS_PASSWORD = {'*' * len(TWS_PASSWORD)}")


# ================================================================
# TEST 1 — NETWORK CONNECTIVITY
# ================================================================
def test_connectivity():
    print()
    print(SEP)
    print("TEST 1 — Network connectivity")
    print(SEP)
    try:
        r = requests.get(
            f"{TWS_HOST}/console/login.jsp",
            verify=False,
            timeout=10
        )
        print(f"OK    Console login page reachable  (HTTP {r.status_code})")
        return True
    except requests.exceptions.ConnectionError:
        print(f"FAIL  Cannot reach {TWS_HOST}")
        print("      Connect to Barclays VPN and retry")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print("FAIL  Timed out — check VPN connection")
        sys.exit(1)


# ================================================================
# TEST 2 — AUTHENTICATION
# ================================================================
def test_auth():
    print()
    print(SEP)
    print("TEST 2 — Authentication")
    print(SEP)
    probe_paths = [
        "/JobManagerRESTWeb/jobs",
        "/twsWaRESTWeb/rest/batchJobs",
        "/wa/rest/batchJobs",
    ]
    for path in probe_paths:
        try:
            r = requests.get(
                f"{TWS_HOST}{path}",
                auth=HTTPBasicAuth(TWS_USER, TWS_PASSWORD),
                verify=False,
                timeout=8
            )
            if r.status_code == 401:
                print("FAIL  HTTP 401 Unauthorized — wrong password")
                print(f"      User: {TWS_USER}")
                print("      Update TWS_PASSWORD in this script")
                sys.exit(1)
            if r.status_code == 403:
                print("FAIL  HTTP 403 Forbidden — no REST API permission")
                print(f"      Ask TWS admin to grant API access for {TWS_USER}")
                sys.exit(1)
            if r.status_code != 404:
                print(f"OK    Credentials accepted  (HTTP {r.status_code})")
                return
        except Exception:
            continue
    print("OK    No 401/403 received — credentials appear valid")


# ================================================================
# TEST 3 — ENDPOINT DISCOVERY
# ================================================================
def discover_working_endpoint():
    print()
    print(SEP)
    print("TEST 3 — Discover correct REST API endpoint")
    print(f"         Trying {len(CANDIDATE_ENDPOINTS)} known patterns...")
    print(SEP)

    working = []

    for path in CANDIDATE_ENDPOINTS:
        url = f"{TWS_HOST}{path}"
        try:
            r = requests.get(
                url,
                auth=HTTPBasicAuth(TWS_USER, TWS_PASSWORD),
                verify=False,
                timeout=8
            )
            ct   = r.headers.get("Content-Type", "")
            text = r.text.strip()
            is_json = "json" in ct or text.startswith("{") or text.startswith("[")
            is_html = "html" in ct or text.lower().startswith("<!doctype") or text.startswith("<html")

            if r.status_code == 200 and is_json:
                print(f"  FOUND JSON   HTTP 200  {path}")
                working.append((path, "json"))
            elif r.status_code == 200 and not is_html:
                print(f"  FOUND        HTTP 200  {path}")
                working.append((path, "other"))
            elif r.status_code == 200:
                print(f"  HTML page    HTTP 200  {path}  (not an API endpoint)")
            elif r.status_code == 400:
                print(f"  EXISTS       HTTP 400  {path}  (needs query parameters)")
                working.append((path, "needs_params"))
            elif r.status_code == 405:
                print(f"  EXISTS       HTTP 405  {path}  (wrong HTTP method — endpoint valid)")
                working.append((path, "wrong_method"))
            elif r.status_code == 404:
                print(f"  404          {path}")
            elif r.status_code == 401:
                print(f"  401 Auth     {path}")
            elif r.status_code == 403:
                print(f"  403 Forbid   {path}")
            else:
                print(f"  {r.status_code}          {path}")
        except requests.exceptions.Timeout:
            print(f"  TIMEOUT      {path}")
        except Exception as e:
            print(f"  ERROR        {path}  ({e})")

    return working


# ================================================================
# TEST 4 — FETCH REAL JOB STATUS
# ================================================================
def test_job_fetch(endpoint_path):
    print()
    print(SEP)
    print("TEST 4 — Fetch real job status from TWS")
    print(f"         Endpoint : {endpoint_path}")
    print(f"         Stream   : {TEST_STREAM}")
    print(f"         Job      : {TEST_JOB}")
    print(SEP)

    url = f"{TWS_HOST}{endpoint_path}"

    # Try all known parameter styles used across TWS versions
    param_styles = [
        {"jobStream": TEST_STREAM, "job": TEST_JOB},
        {"jobStreamName": TEST_STREAM, "jobName": TEST_JOB},
        {"stream": TEST_STREAM, "name": TEST_JOB},
        {"engine": TWS_ENGINE, "jobStream": TEST_STREAM, "job": TEST_JOB},
        {"engine": TWS_ENGINE, "jobstream": TEST_STREAM, "jobname": TEST_JOB},
        {"filter": f"jobstream={TEST_STREAM};job={TEST_JOB}"},
        {},
    ]

    for params in param_styles:
        try:
            r = requests.get(
                url,
                params=params,
                auth=HTTPBasicAuth(TWS_USER, TWS_PASSWORD),
                verify=False,
                timeout=30
            )
            print(f"Params : {params if params else '(none)'}")
            print(f"HTTP   : {r.status_code}")

            if r.status_code == 200:
                try:
                    data = r.json()
                    print("Raw response (first 800 chars):")
                    print(json.dumps(data, indent=2)[:800])
                    print()
                    return data
                except Exception:
                    print(f"Not JSON: {r.text[:200]}")
            else:
                print(f"Response: {r.text[:150]}")
            print()
        except Exception as e:
            print(f"Exception: {e}")
            print()

    return None


# ================================================================
# TEST 5 — PARSE STATUS FIELD
# ================================================================
def test_status_parse(data, endpoint_path):
    print()
    print(SEP)
    print("TEST 5 — Parse status from response")
    print(SEP)

    if data is None:
        print("SKIP  No data returned")
        return None

    status = None
    pattern_used = None

    checks = [
        ("data['jobs'][0]['status']",
         lambda d: d.get("jobs", [{}])[0].get("status") if isinstance(d, dict) and d.get("jobs") else None),
        ("data[0]['status']",
         lambda d: d[0].get("status") if isinstance(d, list) and d else None),
        ("data['status']",
         lambda d: d.get("status") if isinstance(d, dict) else None),
        ("data['jobList'][0]['status']",
         lambda d: d.get("jobList", [{}])[0].get("status") if isinstance(d, dict) and d.get("jobList") else None),
        ("data['jobList'][0]['currentStatus']",
         lambda d: d.get("jobList", [{}])[0].get("currentStatus") if isinstance(d, dict) and d.get("jobList") else None),
        ("data['batchJobs'][0]['status']",
         lambda d: d.get("batchJobs", [{}])[0].get("status") if isinstance(d, dict) and d.get("batchJobs") else None),
        ("data['runs'][0]['status']",
         lambda d: d.get("runs", [{}])[0].get("status") if isinstance(d, dict) and d.get("runs") else None),
        ("data[0]['currentStatus']",
         lambda d: d[0].get("currentStatus") if isinstance(d, list) and d else None),
        ("data[0]['jobStatus']",
         lambda d: d[0].get("jobStatus") if isinstance(d, list) and d else None),
    ]

    for label, fn in checks:
        try:
            val = fn(data)
            if val:
                status = val
                pattern_used = label
                break
        except Exception:
            continue

    if status:
        normalized = normalize_status(status)
        print(f"OK    Pattern       : {pattern_used}")
        print(f"OK    Raw status    : {status}")
        print(f"OK    Normalized    : {normalized}")
    else:
        print("WARN  Could not find status field automatically.")
        print("      Response structure received:")
        if isinstance(data, dict):
            print(f"      Top-level keys : {list(data.keys())}")
            for k, v in data.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    print(f"      data['{k}'][0]  : {list(v[0].keys())}")
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            print(f"      data[0] keys   : {list(data[0].keys())}")
        print()
        print("      Share the keys shown above to fix the parser.")

    return status


def normalize_status(s):
    if not s:
        return "UNKNOWN"
    s = str(s).upper()
    if   s in ["SUCC", "SUCCESS", "COMPLETED"]:      return "SUCCESS"
    elif s in ["ABEND", "FAILED", "FAIL", "ERROR"]:  return "FAILED"
    elif s in ["EXEC", "RUNNING"]:                   return "RUNNING"
    elif s in ["READY"]:                              return "READY"
    elif s in ["WAIT", "WAITING"]:                   return "WAITING"
    elif s in ["HOLD"]:                               return "HOLD"
    else:                                             return "UNKNOWN"


# ================================================================
# FINAL SUMMARY
# ================================================================
def print_summary(working, status):
    print()
    print(SEP)
    print("FINAL SUMMARY")
    print(SEP)

    if not working:
        print("FAIL  No working endpoint found.")
        print()
        print("To find the correct endpoint manually:")
        print(f"  1. Open browser → {TWS_HOST}/console/login.jsp")
        print("  2. Log in → Monitoring & Rep... → Monitor Workload → Run")
        print("  3. Press F12 → Network tab → look for XHR/Fetch requests")
        print("  4. Find the request URL that returns job JSON data")
        print("  5. Add that path to CANDIDATE_ENDPOINTS in this script and re-run")
        return

    print(f"Working endpoints: {len(working)}")
    for path, kind in working:
        print(f"  {path}  [{kind}]")

    best = working[0][0]
    print()

    if status:
        print("Job status fetched and parsed successfully!")
        print()
        print(SEP)
        print("COPY THESE VALUES into update_tws_status_secure.py:")
        print(SEP)
        print(f'  TWS_HOST     = "{TWS_HOST}"')
        print(f'  TWS_ENDPOINT = "{best}"')
        print(f'  TWS_ENGINE   = "{TWS_ENGINE}"')
        print()
        print("  Replace the url line in get_status_from_tws() with:")
        print(f'  url = f"{{TWS_HOST}}{best}"')
        print(SEP)
    else:
        print(f"Endpoint found but status parse needs adjustment.")
        print(f"Share the response keys printed above.")

    print()


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    print()
    print(SEP)
    print("IBM TWS CONNECTION TESTER")
    print(f"Console : {TWS_HOST}/console/login.jsp")
    print(f"Engine  : {TWS_ENGINE} [twsmaster]")
    print(SEP)

    check_credentials()
    test_connectivity()
    test_auth()
    working = discover_working_endpoint()

    data   = None
    status = None

    if working:
        for path, kind in working:
            if kind in ("json", "needs_params", "other"):
                data = test_job_fetch(path)
                if data is not None:
                    status = test_status_parse(data, path)
                    break
        if data is None:
            data = test_job_fetch(working[0][0])
            if data:
                status = test_status_parse(data, working[0][0])
    else:
        print()
        print("No working endpoint found — see summary for next steps.")

    print_summary(working, status)
