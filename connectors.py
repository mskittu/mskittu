"""
Read-only query connectors for Source Target Recon.

Supported: Hive (PyHive / impyla), Hive or Impala through Hue (requests, user's own Hue
login), Oracle (python-oracledb), AWS Athena (boto3), AWS Redshift (redshift_connector),
plus a local SQLite demo.

Every connector:
  * runs ONE read-only statement (SELECT / WITH), checked before it is sent
  * stops after `max_rows` rows and reports truncation
  * honours a timeout and can be cancelled from another thread
  * returns every value as a string so source and target compare like-for-like

Drivers are imported lazily; a missing driver produces a clear message instead of a crash.
"""
import datetime as dt
import html
import json
import uuid
import os
import re
import sqlite3
import threading
import time
from decimal import Decimal


def _use_system_certs():
    """Trust the operating system's certificate store (Windows / macOS / Linux) for HTTPS, e.g. Hue's company
    certificate. On by default on Windows; RECON_USE_SYSTEM_CERTS=0 switches it off, =1 switches it on elsewhere."""
    want = os.environ.get("RECON_USE_SYSTEM_CERTS", "1" if os.name == "nt" else "0") == "1"
    if not want:
        return False
    try:
        import truststore
        truststore.inject_into_ssl()
        return True
    except Exception:
        return False


SYSTEM_CERTS = _use_system_certs()


class QueryError(Exception):
    """An error that is safe to show to the user."""


# ---------------------------------------------------------------- connection types (drives the admin form)
F = lambda key, label, **kw: dict(key=key, label=label, **kw)  # noqa: E731

TYPES = {
    "hive": dict(
        label="Hive", icon="db", test_sql="SELECT 1",
        sql_hint="Type your read-only SELECT query here (use db.table names)",
        fields=[F("host", "HiveServer2 host", required=True), F("port", "Port", default="10000"),
                F("database", "Database", default="default"),
                F("auth", "Authentication", choices=["KERBEROS", "LDAP", "NONE", "NOSASL", "CUSTOM"], default="KERBEROS"),
                F("kerberos_service_name", "Kerberos service name", default="hive"),
                F("transport", "Transport", choices=["binary", "http"], default="binary"),
                F("http_path", "HTTP path (http transport)", default="cliservice"),
                F("use_ssl", "Use SSL (http transport)", choices=["no", "yes"], default="no"),
                F("username", "Username (if fixed)")],
        secret_modes=["none", "signin", "ask", "env", "aws_secret"],
        ask_fields=[F("username", "Username"), F("password", "Password", secret=True)]),
    "hue": dict(
        label="Hive via Hue", icon="db", test_sql="SELECT 1",
        sql_hint="Type your read-only SELECT query here (use db.table names)",
        fields=[F("url", "Hue address", required=True, placeholder="http://huehost.intranet.company.com:8888"),
                F("dialect", "Query engine", choices=["hive", "impala"], default="hive"),
                F("database", "Default database (one you can access; blank = your Hue default)", default=""),
                F("api", "Hue API", choices=["auto", "v1", "classic"], default="auto"),
                F("verify_ssl", "Check HTTPS certificate (https only)", choices=["yes", "no"], default="yes"),
                F("ca_bundle", "Company CA bundle file on the server (optional, https only)"),
                F("null_text", "Treat the text NULL as an empty value", choices=["yes", "no"], default="yes"),
                F("login_server", "Hue sign-in server (the dropdown on the Hue sign-in page, e.g. LDAP; blank = detect)"),
                F("username", "Username (if fixed)")],
        secret_modes=["signin", "ask", "env", "aws_secret"],
        ask_fields=[F("username", "Hue username"), F("password", "Hue password", secret=True)]),
    "oracle": dict(
        label="Oracle", icon="db", test_sql="SELECT 1 FROM dual",
        sql_hint="Type your read-only SELECT query here (use db.table names)",
        fields=[F("host", "Host"), F("port", "Port", default="1521"), F("service_name", "Service name"),
                F("dsn", "Full DSN / TNS descriptor (optional, overrides host/port/service)"),
                F("username", "Username (if fixed)")],
        secret_modes=["ask", "env", "aws_secret"],
        ask_fields=[F("username", "Username"), F("password", "Password", secret=True)]),
    "athena": dict(
        label="AWS Athena", icon="cloud", test_sql="SELECT 1",
        sql_hint="Type your read-only SELECT query here (use db.table names)",
        fields=[F("region", "AWS region", required=True, default="eu-west-1"),
                F("database", "Glue database", required=True), F("catalog", "Data catalog", default="AwsDataCatalog"),
                F("workgroup", "Workgroup", default="primary"),
                F("output_location", "Query results S3 path (if the workgroup has none)"),
                F("aws_profile", "AWS profile on the server (optional)"),
                F("role_arn", "IAM role to assume (optional)"),
                F("console_link", "Show 'Open in AWS console' button (needs the IAM role above)", choices=["no", "yes"], default="no")],
        secret_modes=["none", "ask", "aws_secret"],
        ask_fields=[F("aws_access_key_id", "Access key ID"), F("aws_secret_access_key", "Secret access key", secret=True),
                    F("aws_session_token", "Session token (optional)", secret=True, optional=True)]),
    "redshift": dict(
        label="AWS Redshift", icon="cloud", test_sql="SELECT 1",
        sql_hint="Type your read-only SELECT query here (use db.table names)",
        fields=[F("auth_mode", "Sign-in", choices=["password", "iam"], default="password"),
                F("host", "Endpoint host"), F("port", "Port", default="5439"), F("database", "Database", required=True),
                F("region", "AWS region (IAM)", default="eu-west-1"),
                F("cluster_identifier", "Cluster identifier (IAM, provisioned)"),
                F("serverless_workgroup", "Serverless workgroup (IAM, serverless)"),
                F("aws_profile", "AWS profile on the server (IAM, optional)"),
                F("username", "Database user (if fixed)")],
        secret_modes=["ask", "env", "aws_secret", "none"],
        ask_fields=[F("username", "Username"), F("password", "Password", secret=True)]),
    "sqlite": dict(
        label="SQLite (demo)", icon="db", test_sql="SELECT 1",
        sql_hint="Type your read-only SELECT query here (use db.table names)",
        fields=[F("path", "Database file inside the data folder", required=True)],
        secret_modes=["none"], ask_fields=[]),
}
SECRET_MODE_LABEL = {
    "signin": "Use each person's own Recon sign-in (Hue / LDAP username and password)",
    "none": "No password (Kerberos ticket / IAM role on the server)",
    "ask": "Each user enters their own credentials",
    "env": "Server environment variable",
    "aws_secret": "AWS Secrets Manager",
}


def public_type_info():
    return {k: dict(label=v["label"], icon=v["icon"], fields=v["fields"], secret_modes=v["secret_modes"],
                    ask_fields=v["ask_fields"], sql_hint=v["sql_hint"]) for k, v in TYPES.items()}


# ---------------------------------------------------------------- read-only guard
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|DROP|CREATE|ALTER|TRUNCATE|RENAME|GRANT|REVOKE|"
    r"CALL|EXEC|EXECUTE|BEGIN|COMMIT|ROLLBACK|LOCK|UNLOAD|COPY|VACUUM|ANALYZE|MSCK|LOAD|EXPORT|IMPORT|"
    r"SET|USE|DECLARE|DO|INTO)\b", re.I)


def _strip_sql(sql):
    """Remove comments and string/identifier literals so keyword checks only see real SQL."""
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "-" and sql[i:i + 2] == "--":
            j = sql.find("\n", i)
            i = n if j < 0 else j
            continue
        if c == "/" and sql[i:i + 2] == "/*":
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            out.append(" ")
            continue
        if c in "'\"`":
            j = i + 1
            while j < n:
                if sql[j] == "\\" and c != "`":
                    j += 2
                    continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    break
                j += 1
            out.append(" '' ")
            i = j + 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def check_read_only(sql):
    """Return the statement without a trailing ';' or raise QueryError."""
    if not sql or not sql.strip():
        raise QueryError("Enter a query.")
    if len(sql) > 200_000:
        raise QueryError("The query is too long.")
    bare = _strip_sql(sql).strip()
    while bare.endswith(";"):
        bare = bare[:-1].rstrip()
    if ";" in bare:
        raise QueryError("Run one statement at a time (remove the extra ';').")
    first = re.match(r"\(*\s*(\w+)", bare)
    if not first or first.group(1).upper() not in ("SELECT", "WITH"):
        raise QueryError("Only SELECT queries (or WITH ... SELECT) can be run. This is a read-only validation tool.")
    bad = _FORBIDDEN.search(bare)
    if bad:
        raise QueryError(f"'{bad.group(1).upper()}' is not allowed. Only read-only SELECT queries can be run.")
    clean = sql.strip()
    while clean.endswith(";"):
        clean = clean[:-1].rstrip()
    return clean


# ---------------------------------------------------------------- value normalisation
_TS_MIDNIGHT = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T]00:00:00(\.0+)?$")
_TS_ZERO_FRAC = re.compile(r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\.0+$")


def normalize_value(v, dates=True):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, Decimal):
        s = format(v, "f")
        return s.rstrip("0").rstrip(".") if "." in s else s
    if isinstance(v, float):
        if v != v:
            return "NaN"
        return str(int(v)) if v.is_integer() and abs(v) < 1e15 else repr(v)
    if isinstance(v, dt.datetime):
        if dates and v.hour == v.minute == v.second == v.microsecond == 0 and v.tzinfo is None:
            return v.strftime("%Y-%m-%d")
        return v.isoformat(sep=" ", timespec="seconds" if not v.microsecond else "microseconds")
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, (list, tuple, dict)):
        return json.dumps(v, default=str)
    s = str(v)
    if dates and len(s) <= 30:
        m = _TS_MIDNIGHT.match(s)
        if m:
            return m.group(1)
        m = _TS_ZERO_FRAC.match(s)
        if m:
            return m.group(1).replace("T", " ")
    return s


def _fetch(cur, max_rows, dates, strip_prefix=False):
    if not cur.description:
        raise QueryError("The query returned no result set.")
    cols = [str(d[0]) for d in cur.description]
    if strip_prefix:  # Hive returns "table.column" when hive.resultset.use.unique.column.names=true
        cols = [c.split(".", 1)[1] if "." in c else c for c in cols]
    rows, truncated = [], False
    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        for r in batch:
            if len(rows) >= max_rows:
                truncated = True
                break
            rows.append([normalize_value(v, dates) for v in r])
        if truncated:
            break
    return cols, rows, truncated


# ---------------------------------------------------------------- run context (timeout + cancel)
class RunContext:
    def __init__(self, timeout):
        self.timeout = timeout
        self.deadline = time.time() + timeout
        self.cancelled = threading.Event()
        self._hooks = []
        self._lock = threading.Lock()

    def on_cancel(self, fn):
        with self._lock:
            self._hooks.append(fn)

    def cancel(self):
        self.cancelled.set()
        with self._lock:
            hooks = list(self._hooks)
        for fn in hooks:
            try:
                fn()
            except Exception:
                pass

    def check(self):
        if self.cancelled.is_set():
            raise QueryError("Cancelled.")
        if time.time() > self.deadline:
            raise QueryError(f"Timed out after {self.timeout} seconds.")


def _need(module, pip_name):
    try:
        return __import__(module, fromlist=["_"])
    except ImportError:
        raise QueryError(f"The {pip_name} driver is not installed on the server. "
                         f"Ask your Recon admin to run: pip install {pip_name}")


def _aws_session(cfg, cred, region, assume=True):
    boto3 = _need("boto3", "boto3")
    kw = {"region_name": region}
    if cfg.get("aws_profile"):
        kw["profile_name"] = cfg["aws_profile"]
    if cred.get("aws_access_key_id"):
        kw.update(aws_access_key_id=cred["aws_access_key_id"], aws_secret_access_key=cred.get("aws_secret_access_key"),
                  aws_session_token=cred.get("aws_session_token") or None)
    session = boto3.Session(**kw)
    if assume and cfg.get("role_arn"):
        c = session.client("sts").assume_role(RoleArn=cfg["role_arn"], RoleSessionName="source-target-recon",
                                              DurationSeconds=3600)["Credentials"]
        session = boto3.Session(aws_access_key_id=c["AccessKeyId"], aws_secret_access_key=c["SecretAccessKey"],
                                aws_session_token=c["SessionToken"], region_name=region)
    return session


def aws_console_url(cfg, cred, who, timeout=20):
    """One-click sign-in to the AWS Athena console with the connection's IAM role (no password typed).

    Assumes cfg['role_arn'] with the server's own credentials, swaps the temporary keys for a console
    sign-in token at the AWS federation endpoint, and returns a login URL that opens the Athena query editor.
    The URL works once and only for a few minutes.
    """
    if not cfg.get("role_arn"):
        raise QueryError("The 'Open in AWS console' button needs an IAM role on this connection. Ask your Recon admin.")
    requests = _need("requests", "requests")
    region = cfg.get("region") or "eu-west-1"
    base = _aws_session(cfg, cred, region, assume=False)
    name = re.sub(r"[^\w+=,.@-]", "-", f"recon-{who}")[:64]
    try:
        c = base.client("sts").assume_role(RoleArn=cfg["role_arn"], RoleSessionName=name, DurationSeconds=3600)["Credentials"]
    except Exception as exc:
        raise QueryError(f"AWS refused the IAM role: {str(exc).splitlines()[0][:300]}")
    session = json.dumps({"sessionId": c["AccessKeyId"], "sessionKey": c["SecretAccessKey"], "sessionToken": c["SessionToken"]})
    fed = "https://signin.aws.amazon.com/federation"
    try:
        r = requests.get(fed, params={"Action": "getSigninToken", "Session": session}, timeout=timeout)
        r.raise_for_status()
        token = r.json()["SigninToken"]
    except Exception as exc:
        raise QueryError(f"AWS console sign-in failed: {str(exc).splitlines()[0][:300]}. "
                         "Console sign-in may be switched off for this role, or the server can't reach signin.aws.amazon.com.")
    dest = f"https://{region}.console.aws.amazon.com/athena/home?region={region}#/query-editor"
    from urllib.parse import urlencode
    return fed + "?" + urlencode({"Action": "login", "Issuer": "source-target-recon", "Destination": dest, "SigninToken": token})


# ---------------------------------------------------------------- drivers
def _run_hive(cfg, cred, sql, max_rows, ctx, dates):
    auth = (cfg.get("auth") or "KERBEROS").upper()
    host, port = cfg["host"], int(cfg.get("port") or 10000)
    database = cfg.get("database") or "default"
    user = cred.get("username") or cfg.get("username") or None
    if cfg.get("transport") == "http" or cfg.get("use_ssl") == "yes":
        dbapi = _need("impala.dbapi", "impyla")
        mech = {"NONE": "NOSASL", "NOSASL": "NOSASL", "LDAP": "LDAP", "KERBEROS": "GSSAPI", "CUSTOM": "PLAIN"}.get(auth, "PLAIN")
        conn = dbapi.connect(host=host, port=port, database=database, auth_mechanism=mech,
                             user=user, password=cred.get("password"),
                             use_ssl=cfg.get("use_ssl") == "yes", use_http_transport=cfg.get("transport") == "http",
                             http_path=cfg.get("http_path") or "cliservice",
                             kerberos_service_name=cfg.get("kerberos_service_name") or "hive", timeout=ctx.timeout)
    else:
        hive = _need("pyhive.hive", "pyhive[hive]")
        kw = dict(host=host, port=port, database=database, auth=auth)
        if auth in ("LDAP", "CUSTOM"):
            if not (user and cred.get("password")):
                raise QueryError("This Hive connection needs a username and password.")
            kw.update(username=user, password=cred["password"])
        elif auth == "KERBEROS":
            kw["kerberos_service_name"] = cfg.get("kerberos_service_name") or "hive"
        elif user:
            kw["username"] = user
        conn = hive.Connection(**kw)
    try:
        cur = conn.cursor()
        ctx.on_cancel(lambda: (getattr(cur, "cancel", None) or getattr(cur, "cancel_operation"))())
        ctx.check()
        cur.execute(sql)
        ctx.check()
        return _fetch(cur, max_rows, dates, strip_prefix=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _run_oracle(cfg, cred, sql, max_rows, ctx, dates):
    oracledb = _need("oracledb", "oracledb")
    oracledb.defaults.fetch_lobs = False      # CLOB -> str
    oracledb.defaults.fetch_decimals = True   # NUMBER -> exact Decimal
    dsn = cfg.get("dsn") or oracledb.makedsn(cfg["host"], int(cfg.get("port") or 1521), service_name=cfg.get("service_name"))
    user = cred.get("username") or cfg.get("username")
    if not user or not cred.get("password"):
        raise QueryError("This Oracle connection needs a username and password.")
    conn = oracledb.connect(user=user, password=cred["password"], dsn=dsn)
    try:
        conn.call_timeout = int(ctx.timeout * 1000)
        ctx.on_cancel(conn.cancel)
        cur = conn.cursor()
        cur.arraysize = 5000
        cur.execute("SET TRANSACTION READ ONLY")
        ctx.check()
        cur.execute(sql)
        return _fetch(cur, max_rows, dates)
    finally:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass


def _run_athena(cfg, cred, sql, max_rows, ctx, dates):
    region = cfg.get("region") or "eu-west-1"
    ath = _aws_session(cfg, cred, region).client("athena", region_name=region)
    params = dict(QueryString=sql, WorkGroup=cfg.get("workgroup") or "primary",
                  QueryExecutionContext={"Database": cfg["database"], "Catalog": cfg.get("catalog") or "AwsDataCatalog"})
    if cfg.get("output_location"):
        params["ResultConfiguration"] = {"OutputLocation": cfg["output_location"]}
    qid = ath.start_query_execution(**params)["QueryExecutionId"]
    ctx.on_cancel(lambda: ath.stop_query_execution(QueryExecutionId=qid))
    delay = 0.4
    while True:
        status = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise QueryError(f"Athena query {state.lower()}: {status.get('StateChangeReason', 'no reason given')}")
        try:
            ctx.check()
        except QueryError:
            try:
                ath.stop_query_execution(QueryExecutionId=qid)
            except Exception:
                pass
            raise
        time.sleep(delay)
        delay = min(delay * 1.5, 3.0)
    cols, rows, truncated, first = None, [], False, True
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid, PaginationConfig={"PageSize": 1000}):
        rs = page["ResultSet"]
        if cols is None:
            cols = [c["Name"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
        data = rs.get("Rows", [])
        if first:
            first = False
            if data and [d.get("VarCharValue", "") for d in data[0]["Data"]] == cols:
                data = data[1:]  # Athena repeats the header as the first row of a SELECT
        for r in data:
            if len(rows) >= max_rows:
                truncated = True
                break
            rows.append([normalize_value(d.get("VarCharValue"), dates) for d in r["Data"]])
        if truncated:
            break
        ctx.check()
    return cols or [], rows, truncated


def _run_redshift(cfg, cred, sql, max_rows, ctx, dates):
    rc = _need("redshift_connector", "redshift_connector")
    user = cred.get("username") or cfg.get("username")
    if cfg.get("auth_mode") == "iam":
        kw = dict(iam=True, database=cfg["database"], db_user=user, region=cfg.get("region") or "eu-west-1")
        if cfg.get("host"):
            kw.update(host=cfg["host"], port=int(cfg.get("port") or 5439))
        if cfg.get("cluster_identifier"):
            kw["cluster_identifier"] = cfg["cluster_identifier"]
        if cfg.get("serverless_workgroup"):
            kw.update(is_serverless=True, serverless_work_group=cfg["serverless_workgroup"])
        if cfg.get("aws_profile"):
            kw["profile"] = cfg["aws_profile"]
        conn = rc.connect(**kw)
    else:
        if not user or not cred.get("password"):
            raise QueryError("This Redshift connection needs a username and password.")
        conn = rc.connect(host=cfg["host"], port=int(cfg.get("port") or 5439), database=cfg["database"],
                          user=user, password=cred["password"], ssl=True, timeout=ctx.timeout)
    try:
        ctx.on_cancel(conn.close)
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout TO {int(ctx.timeout * 1000)}")
        ctx.check()
        cur.execute(sql)
        return _fetch(cur, max_rows, dates)
    finally:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass


def _run_sqlite(cfg, cred, sql, max_rows, ctx, dates, data_dir=None):
    base = os.path.realpath(data_dir or ".")
    path = os.path.realpath(os.path.join(base, cfg["path"]))
    if not path.startswith(base + os.sep) or not os.path.exists(path):
        raise QueryError("The demo database file was not found in the data folder.")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    try:
        ctx.on_cancel(conn.interrupt)
        cur = conn.cursor()
        cur.execute(sql)
        return _fetch(cur, max_rows, dates)
    except sqlite3.OperationalError as exc:
        raise QueryError(str(exc))
    finally:
        conn.close()


# ---------------------------------------------------------------- Hue (Hive / Impala through the Hue web API)
class _Hue:
    """Signs in to Hue with the user's own username and password and runs one statement.

    api="v1":      Hue 4.9+ public API (POST /api/v1/token/auth, then /api/v1/editor/...)
    api="classic": older Hue (Django sign-in form, then /notebook/api/...)
    api="auto":    try v1 first, fall back to classic.
    """

    def __init__(self, cfg, cred, ctx):
        requests = _need("requests", "requests")
        self.rq = requests
        self.base = cfg["url"].strip().rstrip("/")
        if not re.match(r"^https?://", self.base, re.I):
            self.base = "http://" + self.base
        if self.base.endswith("/hue"):
            self.base = self.base[:-4]
        for tail in ("/hue/accounts/login", "/hue/account/login", "/accounts/login"):
            if self.base.endswith(tail):
                self.base = self.base[: -len(tail)]
        if not re.match(r"^https?://", self.base, re.I):
            raise QueryError("The Hue address must start with http:// or https://")
        self.cfg, self.ctx = cfg, ctx
        self.dialect = (cfg.get("dialect") or "hive").lower()
        self.user = cred.get("username") or cfg.get("username")
        self.password = cred.get("password")
        if not (self.user and self.password):
            raise QueryError("Enter your Hue username and password.")
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json", "User-Agent": "source-target-recon"})
        if cfg.get("ca_bundle"):
            self.s.verify = cfg["ca_bundle"]
        elif cfg.get("verify_ssl") == "no":
            self.s.verify = False
            try:
                requests.packages.urllib3.disable_warnings()
            except Exception:
                pass
        self.mode = None
        self.op = None           # v1: operation id
        self.nb = self.snip = None  # classic: notebook + snippet

    # ---- http helpers
    def _timeout(self):
        return max(5.0, min(60.0, self.ctx.deadline - time.time()))

    def _post(self, path, data, ok404=False):
        self.ctx.check()
        try:
            r = self.s.post(self.base + path, data=data, timeout=self._timeout(),
                            headers={"Referer": self.base + "/"} if self.mode == "classic" else None)
        except self.rq.exceptions.SSLError:
            raise QueryError("Hue HTTPS certificate was not trusted. Set the company CA bundle on the connection "
                             "(or RECON_HUE_CA_BUNDLE), or switch certificate checking off.")
        except self.rq.exceptions.ConnectionError:
            raise QueryError(f"Could not reach Hue at {self.base}. Check the address and that the Recon server can reach it.")
        except self.rq.exceptions.Timeout:
            raise QueryError("Hue did not answer in time.")
        if ok404 and r.status_code in (404, 405):
            return None
        if r.status_code in (401, 403):
            raise QueryError("Hue refused the request: check your Hue username and password, and that you can run "
                             f"{self.dialect} queries in Hue.")
        try:
            body = r.json()
        except ValueError:
            if "login" in (r.url or "") or "<form" in r.text[:5000].lower():
                raise QueryError("Hue sign-in failed: check your Hue username and password.")
            raise QueryError(f"Hue returned an unexpected answer (HTTP {r.status_code}) from {path}.")
        if isinstance(body, dict) and body.get("status") not in (None, 0) and (body.get("message") or body.get("error")):
            raise QueryError(f"Hue: {_hue_msg(body.get('message') or body.get('error'))} [{path.split('/')[-1] or path}]")
        if r.status_code >= 400:
            raise QueryError(f"Hue error (HTTP {r.status_code}) from {path}.")
        return body

    # ---- sign in
    def _login_page(self):
        """GET the classic sign-in page. Returns (path, html) or (None, None)."""
        for path in ("/hue/accounts/login/", "/accounts/login/"):
            self.ctx.check()
            try:
                r = self.s.get(self.base + path, timeout=self._timeout())
            except self.rq.exceptions.SSLError:
                raise QueryError("Hue HTTPS certificate was not trusted. Set the company CA bundle on the connection "
                                 "(or RECON_HUE_CA_BUNDLE), or switch certificate checking off.")
            except self.rq.exceptions.RequestException:
                raise QueryError(f"Could not reach Hue at {self.base}. Check the address and that the Recon server can reach it.")
            if r.status_code != 404:
                return path, r.text
        return None, None

    def _server_choice(self, page):
        """Value for Hue's 'server' dropdown (several LDAP servers), or None when the page has none."""
        m = re.search(r'<select[^>]*name=["\']server["\'][^>]*>(.*?)</select>', page or "", re.S | re.I)
        if not m:
            return None
        opts = re.findall(r'<option[^>]*value=["\']([^"\']*)["\'][^>]*>', m.group(1), re.I)
        want = (self.cfg.get("login_server") or "").strip()
        if want:
            return next((o for o in opts if o.lower() == want.lower()), want)
        sel = re.search(r'<option[^>]*value=["\']([^"\']*)["\'][^>]*selected', m.group(1), re.I)
        return sel.group(1) if sel else (opts[0] if opts else None)

    def login(self):
        want = (self.cfg.get("api") or "auto").lower()
        self._page = (None, None)
        if want == "auto":
            # One password attempt only (LDAP lock-out): if the Hue page asks for a sign-in server, use the classic form.
            self._page = self._login_page()
            if self._server_choice(self._page[1]) is not None:
                want = "classic"
        if want in ("auto", "v1"):
            self.mode = "v1"
            data = {"username": self.user, "password": self.password}
            if self.cfg.get("login_server"):
                data["server"] = self.cfg["login_server"]
            try:
                body = self._post("/api/v1/token/auth", data, ok404=want == "auto")
            except QueryError as exc:
                if "refused" in str(exc):
                    raise QueryError("Hue sign-in failed: check your Hue username and password.")
                raise
            if body is not None:
                tok = body.get("access") if isinstance(body, dict) else None
                if not tok:
                    raise QueryError("Hue sign-in failed: check your Hue username and password.")
                self.s.headers["Authorization"] = "Bearer " + tok
                return
        self.mode = "classic"
        self._classic_login()

    def _classic_login(self):
        path, page = self._page if getattr(self, "_page", (None, None))[0] else self._login_page()
        if not path:
            raise QueryError("Could not find the Hue sign-in page. Check the Hue address.")
        token = self.s.cookies.get("csrftoken")
        if not token:
            m = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)', page or "")
            token = m.group(1) if m else ""
        data = {"username": self.user, "password": self.password, "csrfmiddlewaretoken": token, "next": "/"}
        server = self._server_choice(page)
        if server is not None:
            data["server"] = server
        self.ctx.check()
        try:
            r = self.s.post(self.base + path, timeout=self._timeout(), allow_redirects=True, data=data,
                            headers={"Referer": self.base + path, "X-CSRFToken": token})
        except self.rq.exceptions.RequestException:
            raise QueryError(f"Could not reach Hue at {self.base}. Check the address and that the Recon server can reach it.")
        if self.s.cookies.get("sessionid") and "accounts/login" not in r.url:
            self.s.headers["X-CSRFToken"] = self.s.cookies.get("csrftoken") or token
            self.s.headers["X-Requested-With"] = "XMLHttpRequest"
            return
        raise QueryError("Hue sign-in failed: check your Hue username and password.")

    # ---- statement lifecycle
    def _snippet(self, sql):
        sid = str(uuid.uuid4())
        self.snip = {"id": sid, "type": self.dialect, "status": "running", "statementType": "text",
                     "statement": sql, "statement_raw": sql, "statementPath": "", "aceCursorPosition": None,
                     "associatedDocumentUuid": None, "database": self.cfg.get("database") or None,
                     "properties": {"files": [], "functions": [], "arguments": [], "settings": []},
                     "result": {"id": str(uuid.uuid4()), "type": "table", "handle": {}}, "variables": [],
                     "wasBatchExecuted": False}
        self.nb = {"id": None, "uuid": str(uuid.uuid4()), "name": "", "description": "", "isSaved": False,
                   "isManaged": False, "parentSavedQueryUuid": None, "type": "query-" + self.dialect,
                   "snippets": [], "sessions": []}

    def _classic_data(self, **extra):
        nb = dict(self.nb, snippets=[self.snip])  # Hue reads notebook['snippets'][0] in several versions
        d = {"notebook": json.dumps(nb), "snippet": json.dumps(self.snip)}
        d.update(extra)
        return d

    def execute(self, sql):
        if self.mode == "v1":
            data = {"statement": sql}
            if self.cfg.get("database"):
                data["database"] = self.cfg["database"]
            body = self._post(f"/api/v1/editor/execute/{self.dialect}", data)
            self.op = body.get("history_uuid") or (body.get("handle") or {}).get("guid")
            if not self.op:
                raise QueryError("Hue did not return a query id.")
        else:
            self._snippet(sql)
            try:
                body = self._post(f"/notebook/api/execute/{self.dialect}", self._classic_data())
            except QueryError as first:
                # Some Hue versions reject the classic notebook call: try the newer editor API with the same sign-in.
                try:
                    alt = self._post(f"/api/v1/editor/execute/{self.dialect}",
                                     dict({"statement": sql}, **({"database": self.cfg["database"]} if self.cfg.get("database") else {})), ok404=True)
                except QueryError:
                    alt = None
                op = alt and (alt.get("history_uuid") or (alt.get("handle") or {}).get("guid"))
                if not op:
                    raise first
                self.mode, self.op = "v1", op
                return
            if body.get("history_uuid"):
                self.nb["uuid"] = body["history_uuid"]
            handle = body.get("handle") or {}
            self.snip["result"]["handle"] = handle
            if body.get("history_id"):
                self.nb["id"] = body["history_id"]
            if handle.get("has_result_set") is False:
                raise QueryError("The query returned no result set.")

    def status(self):
        if self.mode == "v1":
            body = self._post("/api/v1/editor/check_status", {"operationId": self.op})
        else:
            body = self._post("/notebook/api/check_status", self._classic_data())
        return ((body.get("query_status") or {}).get("status") or "").lower(), body

    def fetch(self, start_over):
        rows = "1000"
        if self.mode == "v1":
            body = self._post("/api/v1/editor/fetch_result_data",
                              {"operationId": self.op, "rows": rows, "startOver": "true" if start_over else "false"})
        else:
            body = self._post("/notebook/api/fetch_result_data",
                              self._classic_data(rows=rows, startOver="true" if start_over else "false"))
        return body.get("result") or {}

    def cancel(self):
        try:
            if self.mode == "v1" and self.op:
                self.s.post(self.base + "/api/v1/editor/cancel_statement", data={"operationId": self.op}, timeout=10)
            elif self.snip:
                self.s.post(self.base + "/notebook/api/cancel_statement", data=self._classic_data(), timeout=10)
        except Exception:
            pass

    def close(self):
        try:
            if self.mode == "v1" and self.op:
                self.s.post(self.base + "/api/v1/editor/close_statement", data={"operationId": self.op}, timeout=10)
            elif self.snip and self.snip["result"]["handle"]:
                self.s.post(self.base + "/notebook/api/close_statement", data=self._classic_data(), timeout=10)
        except Exception:
            pass
        try:
            self.s.close()
        except Exception:
            pass


def _hue_msg(m):
    m = html.unescape(str(m)).strip()
    m = re.sub(r"<[^>]+>", " ", m)
    m = re.sub(r"\s+", " ", m)
    return m[:500]


def _hue_value(v, null_text, dates):
    if isinstance(v, str):
        v = html.unescape(v).replace("\xa0", " ")   # Hue HTML-escapes results and turns spaces into &nbsp;
        if null_text and v == "NULL":
            return ""
    return normalize_value(v, dates)


def hue_check_login(cfg, username, password, timeout=30):
    """Verify a Hue username/password (used for Recon sign-in). Raises QueryError."""
    hue = _Hue(cfg, {"username": username, "password": password}, RunContext(timeout))
    try:
        hue.login()
    finally:
        hue.close()


def _run_hue(cfg, cred, sql, max_rows, ctx, dates):
    hue = _Hue(cfg, cred, ctx)
    null_text = cfg.get("null_text", "yes") != "no"
    try:
        hue.login()
        hue.execute(sql)
        ctx.on_cancel(hue.cancel)
        delay = 0.5
        while True:
            state, body = hue.status()
            if state in ("available", "success", "succeeded", "finished"):
                break
            if state in ("failed", "expired", "canceled", "cancelled", "closed"):
                raise QueryError(f"Hue query {state}: {_hue_msg(body.get('message') or 'see the query log in Hue')}")
            try:
                ctx.check()
            except QueryError:
                hue.cancel()
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 3.0)
        cols, rows, truncated, first = None, [], False, True
        while True:
            res = hue.fetch(first)
            first = False
            if cols is None:
                cols = [str(m.get("name", "")) for m in (res.get("meta") or [])]
                cols = [c.split(".", 1)[1] if "." in c else c for c in cols]
            for r in res.get("data") or []:
                if len(rows) >= max_rows:
                    truncated = True
                    break
                rows.append([_hue_value(v, null_text, dates) for v in r])
            if truncated or not res.get("has_more") or not res.get("data"):
                break
            ctx.check()
        if cols is None:
            raise QueryError("The query returned no result set.")
        return cols, rows, truncated
    finally:
        hue.close()


DRIVERS = {"hive": _run_hive, "hue": _run_hue, "oracle": _run_oracle, "athena": _run_athena, "redshift": _run_redshift, "sqlite": _run_sqlite}


def run_query(conn_type, cfg, cred, sql, max_rows, ctx, dates=True, data_dir=None):
    """Run one read-only query. Returns dict(columns, rows, truncated, seconds)."""
    if conn_type not in DRIVERS:
        raise QueryError(f"Unknown connection type '{conn_type}'.")
    clean = check_read_only(sql)
    started = time.time()
    try:
        if conn_type == "sqlite":
            cols, rows, truncated = _run_sqlite(cfg, cred, clean, max_rows, ctx, dates, data_dir=data_dir)
        else:
            cols, rows, truncated = DRIVERS[conn_type](cfg, cred, clean, max_rows, ctx, dates)
    except QueryError:
        raise
    except Exception as exc:  # driver errors: keep the message, drop the stack
        if ctx.cancelled.is_set():
            raise QueryError("Cancelled.")
        msg = str(exc).strip().splitlines()[0] if str(exc).strip() else exc.__class__.__name__
        raise QueryError(f"{TYPES[conn_type]['label']} error: {msg[:500]}")
    if len(set(c.lower() for c in cols)) != len(cols):
        raise QueryError("The query returns two columns with the same name. Give each column a unique alias.")
    return dict(columns=cols, rows=rows, truncated=truncated, seconds=round(time.time() - started, 2))


# ---------------------------------------------------------------- DDL fetch (fixed, read-only metadata queries)
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*(\.[A-Za-z_][A-Za-z0-9_$#]*){0,2}$")


def parse_table_list(text):
    if ";" in (text or ""):
        raise QueryError("Separate table names with commas, not ';'.")
    names = [t.strip().strip("`\"") for t in re.split(r"[,\s]+", text or "") if t.strip()]
    if not names:
        raise QueryError("Enter at least one table name, e.g. imp.loan_impairments")
    if len(names) > 20:
        raise QueryError("Fetch up to 20 tables at a time.")
    bad = [n for n in names if not _TABLE_RE.match(n)]
    if bad:
        raise QueryError(f"Not a valid table name: {bad[0]}. Use schema.table with letters, digits and _.")
    return names


def _split(name, default_schema=None):
    parts = name.split(".")
    return (parts[-2] if len(parts) >= 2 else default_schema), parts[-1]


def _oracle_type(dt_, length, prec, scale, char_len):
    t = (dt_ or "").upper()
    if t in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"):
        return f"{t}({char_len or length})"
    if t == "NUMBER":
        if prec in ("", None):
            return "NUMBER" if scale in ("", None, "0") else f"NUMBER(38,{scale})"
        return f"NUMBER({prec},{scale})" if scale not in ("", None, "0") else f"NUMBER({prec})"
    if t == "FLOAT" and prec:
        return f"FLOAT({prec})"
    if t == "RAW":
        return f"RAW({length})"
    return t


def _pg_type(dt_, clen, prec, scale):
    t = (dt_ or "").lower()
    if t == "character varying":
        return f"VARCHAR({clen})" if clen else "VARCHAR"
    if t == "character":
        return f"CHAR({clen})" if clen else "CHAR"
    if t == "numeric":
        return f"NUMERIC({prec},{scale})" if prec else "NUMERIC"
    return {"timestamp without time zone": "TIMESTAMP", "timestamp with time zone": "TIMESTAMPTZ",
            "time without time zone": "TIME"}.get(t, t.upper())


def _glue_ddl(session, region, database, table):
    glue = session.client("glue", region_name=region)
    try:
        t = glue.get_table(DatabaseName=database, Name=table)["Table"]
    except Exception as exc:
        raise QueryError(f"Glue could not read {database}.{table}: {str(exc).splitlines()[0][:200]}")
    sd = t.get("StorageDescriptor", {}) or {}
    col = lambda c: f"  `{c['Name']}` {c['Type']}" + (f" COMMENT '{c['Comment']}'" if c.get("Comment") else "")  # noqa: E731
    lines = [col(c) for c in sd.get("Columns", [])]
    ddl = f"CREATE EXTERNAL TABLE `{database}`.`{t['Name']}` (\n" + ",\n".join(lines) + "\n)"
    if t.get("PartitionKeys"):
        ddl += "\nPARTITIONED BY (\n" + ",\n".join(col(c) for c in t["PartitionKeys"]) + "\n)"
    lib = ((sd.get("SerdeInfo") or {}).get("SerializationLibrary") or "") + " " + (sd.get("InputFormat") or "")
    fmt = next((f for k, f in (("parquet", "PARQUET"), ("orc", "ORC"), ("avro", "AVRO"), ("json", "JSONFILE"),
                               ("TextInputFormat", "TEXTFILE"), ("OpenCSV", "TEXTFILE")) if k.lower() in lib.lower()), "")
    if fmt:
        ddl += f"\nSTORED AS {fmt}"
    if sd.get("Location"):
        ddl += f"\nLOCATION '{sd['Location']}'"
    params = t.get("Parameters") or {}
    keep = {k: v for k, v in params.items() if k in ("table_type", "classification", "format", "write_compression", "parquet.compression")}
    if keep:
        ddl += "\nTBLPROPERTIES (" + ", ".join(f"'{k}'='{v}'" for k, v in sorted(keep.items())) + ")"
    return ddl + ";"


def fetch_ddl(conn_type, cfg, cred, tables, ctx, data_dir=None):
    """Return CREATE TABLE text for each table, using fixed metadata queries (never user SQL)."""
    names = parse_table_list(tables)
    out, missing = [], []

    def rows_of(sql):
        if conn_type == "sqlite":
            return _run_sqlite(cfg, cred, sql, 5000, ctx, False, data_dir=data_dir)[1]
        return DRIVERS[conn_type](cfg, cred, sql, 5000, ctx, False)[1]

    try:
        for name in names:
            ctx.check()
            if conn_type in ("hive", "hue"):
                text = "\n".join(r[0] for r in rows_of(f"SHOW CREATE TABLE {name}"))
                out.append(text.rstrip().rstrip(";") + ";")
            elif conn_type == "oracle":
                owner, tbl = _split(name)
                owner_sql = f"'{owner.upper()}'" if owner else "SYS_CONTEXT('USERENV','CURRENT_SCHEMA')"
                cols = rows_of("SELECT column_name, data_type, data_length, data_precision, data_scale, nullable, char_length "
                               f"FROM all_tab_columns WHERE owner = {owner_sql} AND table_name = '{tbl.upper()}' ORDER BY column_id")
                if not cols:
                    missing.append(name)
                    continue
                pk = [r[0] for r in rows_of(
                    "SELECT cc.column_name FROM all_constraints c JOIN all_cons_columns cc "
                    "ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name "
                    f"WHERE c.constraint_type = 'P' AND c.owner = {owner_sql} AND c.table_name = '{tbl.upper()}' ORDER BY cc.position")]
                body = [f"  {c[0]} {_oracle_type(c[1], c[2], c[3], c[4], c[6])}{' NOT NULL' if c[5] == 'N' else ''}" for c in cols]
                if pk:
                    body.append(f"  PRIMARY KEY ({', '.join(pk)})")
                out.append(f"CREATE TABLE {(owner or 'CURRENT_SCHEMA').upper()}.{tbl.upper()} (\n" + ",\n".join(body) + "\n);")
            elif conn_type == "athena":
                region = cfg.get("region") or "eu-west-1"
                db_, tbl = _split(name, cfg.get("database"))
                out.append(_glue_ddl(_aws_session(cfg, cred, region), region, db_, tbl))
            elif conn_type == "redshift":
                schema, tbl = _split(name, "public")
                cols = rows_of("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, "
                               "is_nullable, column_default FROM information_schema.columns "
                               f"WHERE table_schema = '{schema.lower()}' AND table_name = '{tbl.lower()}' ORDER BY ordinal_position")
                if not cols:
                    missing.append(name)
                    continue
                pk = [r[0] for r in rows_of(
                    "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                    "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "
                    "AND tc.table_schema = kcu.table_schema "
                    f"WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = '{schema.lower()}' "
                    f"AND tc.table_name = '{tbl.lower()}' ORDER BY kcu.ordinal_position")]
                body = [f"  {c[0]} {_pg_type(c[1], c[2], c[3], c[4])}{' NOT NULL' if c[5] == 'NO' else ''}"
                        + (f" DEFAULT {c[6]}" if c[6] and not str(c[6]).startswith('"identity"') else "") for c in cols]
                if pk:
                    body.append(f"  PRIMARY KEY ({', '.join(pk)})")
                out.append(f"CREATE TABLE {schema.lower()}.{tbl.lower()} (\n" + ",\n".join(body) + "\n);")
            elif conn_type == "sqlite":
                r = rows_of(f"SELECT sql FROM sqlite_master WHERE type = 'table' AND lower(name) = lower('{_split(name)[1]}')")
                if not r:
                    missing.append(name)
                    continue
                out.append(r[0][0].rstrip(";") + ";")
            else:
                raise QueryError("DDL fetch is not available for this connection type.")
    except QueryError:
        raise
    except Exception as exc:
        msg = str(exc).strip().splitlines()[0] if str(exc).strip() else exc.__class__.__name__
        raise QueryError(f"{TYPES[conn_type]['label']} error: {msg[:400]}")
    if missing and not out:
        raise QueryError(f"Not found, or no access: {', '.join(missing)}")
    return dict(ddl="\n\n".join(out), tables=len(out), missing=missing)


# ---------------------------------------------------------------- Partitions tab (Hive / Hue): partitions, HDFS folder, MSCK REPAIR
def list_partitions(conn_type, cfg, cred, table, ctx):
    """SHOW PARTITIONS for one table (fixed statement, table name validated)."""
    if conn_type not in ("hive", "hue", "sqlite"):
        raise QueryError("Partitions can be listed for Hive connections (direct or through Hue).")
    name = parse_table_list(table)
    if len(name) != 1:
        raise QueryError("Enter one table, e.g. imp.mds_monthly")
    name = name[0]
    try:
        if conn_type == "sqlite":  # demo: pretend the cob_date values are partitions
            rows = _run_sqlite(cfg, cred, f"SELECT DISTINCT 'cob_date=' || cob_date FROM {name.split('.')[-1]} ORDER BY 1", 100000, ctx, False,
                               data_dir=cfg.get("_data_dir"))[1]
        else:
            rows = DRIVERS[conn_type](cfg, cred, f"SHOW PARTITIONS {name}", 100000, ctx, False)[1]
        loc = ""
        if conn_type != "sqlite":
            ddl = "\n".join(r[0] for r in DRIVERS[conn_type](cfg, cred, f"SHOW CREATE TABLE {name}", 5000, ctx, False)[1])
            m = re.search(r"LOCATION\s*'([^']+)'", ddl, re.I)
            loc = m.group(1) if m else ""
    except QueryError:
        raise
    except Exception as exc:
        raise QueryError(f"{TYPES[conn_type]['label']} error: {str(exc).splitlines()[0][:400] if str(exc) else exc.__class__.__name__}")
    return dict(table=name, partitions=[r[0] for r in rows if r and r[0]], location=loc)


def _hdfs_path(path):
    p = (path or "").strip()
    m = re.match(r"^(?:hdfs|viewfs|s3a?)://[^/]*(/.*)?$", p, re.I)
    if m:
        p = m.group(1) or "/"
    if not p.startswith("/") or ".." in p.split("/") or len(p) > 1000 or re.search(r"[\x00-\x1f]", p):
        raise QueryError("Enter an HDFS folder like /user/hive/warehouse/imp.db/mds_monthly")
    return p.rstrip("/") or "/"


def hue_list_dir(cfg, cred, path, ctx):
    """List one HDFS folder through Hue's File Browser, as the signed-in person (read-only)."""
    from urllib.parse import quote
    p = _hdfs_path(path)
    hue = _Hue(cfg, cred, ctx)
    try:
        hue.login()
        tries = (["/api/v1/storage/view=" + quote(p), "/filebrowser/view=" + quote(p)] if hue.mode == "v1"
                 else ["/filebrowser/view=" + quote(p), "/api/v1/storage/view=" + quote(p)])
        body, last = None, ""
        for t in tries:
            ctx.check()
            try:
                r = hue.s.get(hue.base + t, params={"format": "json", "pagesize": 1000, "pagenum": 1}, timeout=hue._timeout())
            except Exception as exc:
                last = str(exc).splitlines()[0][:200]
                continue
            if r.status_code in (404, 405):
                last = f"HTTP {r.status_code}"
                continue
            try:
                body = r.json()
            except ValueError:
                last = f"HTTP {r.status_code}, not JSON"
                continue
            if r.status_code >= 400 or (isinstance(body, dict) and body.get("status") not in (None, 0) and body.get("message")):
                raise QueryError("Hue: " + _hue_msg((body or {}).get("message") or f"HTTP {r.status_code}"))
            break
        if body is None:
            raise QueryError(f"Hue's File Browser did not answer ({last}). Check that File Browser is enabled in your Hue.")
        out = []
        for f in body.get("files") or []:
            name = f.get("name") or (f.get("path") or "").rstrip("/").split("/")[-1]
            if name in (".", "..", ""):
                continue
            st = f.get("stats") or {}
            out.append(dict(name=name, path=f.get("path") or (p.rstrip("/") + "/" + name), type=f.get("type") or ("dir" if st.get("isDir") else "file"),
                            size=st.get("size", f.get("size")), mtime=st.get("mtime", f.get("mtime")), user=st.get("user", ""), group=st.get("group", "")))
        return dict(path=p, entries=out)
    finally:
        hue.close()


def run_msck(conn_type, cfg, cred, table, ctx):
    """MSCK REPAIR TABLE (changes the Hive metastore). The statement is built here from a validated name."""
    if conn_type not in ("hive", "hue"):
        raise QueryError("MSCK REPAIR runs on Hive connections only.")
    names = parse_table_list(table)
    if len(names) != 1:
        raise QueryError("Enter one table.")
    sql = f"MSCK REPAIR TABLE {names[0]}"
    if conn_type == "hue":
        hue = _Hue(cfg, cred, ctx)
        try:
            hue.login(); hue.execute(sql); ctx.on_cancel(hue.cancel)
            while True:
                state, body = hue.status()
                if state in ("available", "success", "succeeded", "finished"):
                    break
                if state in ("failed", "expired", "canceled", "cancelled", "closed"):
                    raise QueryError(f"Hue: MSCK REPAIR {state}: {_hue_msg(body.get('message') or '')}")
                ctx.check(); time.sleep(1.5)
        finally:
            hue.close()
    else:
        hive = _need("pyhive.hive", "pyhive[hive]")
        auth = (cfg.get("auth") or "KERBEROS").upper()
        kw = dict(host=cfg["host"], port=int(cfg.get("port") or 10000), database=cfg.get("database") or "default", auth=auth)
        if auth in ("LDAP", "CUSTOM"):
            kw.update(username=cred.get("username") or cfg.get("username"), password=cred.get("password"))
        elif auth == "KERBEROS":
            kw["kerberos_service_name"] = cfg.get("kerberos_service_name") or "hive"
        conn = hive.Connection(**kw)
        try:
            conn.cursor().execute(sql)
        finally:
            conn.close()
    return sql
