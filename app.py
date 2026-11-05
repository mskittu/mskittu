"""
Source Target Recon - multi-user web app.

Each user signs in with their own account and gets their own run history,
saved Excel reports and saved rule presets. Admins manage users and can see
everyone's activity.

Files are compared in the user's browser; only the results (summary + Excel
report) are stored on the server.

Run (development):   python app.py
Run (production):    waitress-serve --host 0.0.0.0 --port 8080 app:app
"""
import hashlib
import json
import re
import concurrent.futures
import os
import secrets
import sqlite3
import time
import uuid
import threading
from datetime import datetime, timedelta, timezone
from functools import wraps

import sys as _sys
# Packages bundled in ./vendor are used when they are not installed (no pip / internet needed on locked-down laptops)
_VENDOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor")
if os.path.isdir(_VENDOR) and _VENDOR not in _sys.path:
    _sys.path.append(_VENDOR)

from flask import (Flask, abort, g, jsonify, redirect, render_template, request,
                   send_file, session, url_for)
from werkzeug.security import check_password_hash, generate_password_hash

import connectors as C

BASE = os.path.dirname(os.path.abspath(__file__))


def _load_env_file():
    """Read KEY=VALUE lines from .env next to app.py (real environment variables win)."""
    path = os.path.join(BASE, ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


_load_env_file()
C.SYSTEM_CERTS = C._use_system_certs() or C.SYSTEM_CERTS  # .env may switch the Windows certificate store on or off
DATA_DIR = os.environ.get("RECON_DATA_DIR", os.path.join(BASE, "data"))
DB_PATH = os.path.join(DATA_DIR, "recon.db")
REPORT_DIR = os.path.join(DATA_DIR, "reports")
os.makedirs(REPORT_DIR, exist_ok=True)


def _secret_key():
    env = os.environ.get("RECON_SECRET_KEY")
    if env:
        return env
    path = os.path.join(DATA_DIR, "secret.key")
    if not os.path.exists(path):
        with open(path, "w") as f:
            f.write(secrets.token_hex(32))
        os.chmod(path, 0o600)
    with open(path) as f:
        return f.read().strip()


app = Flask(__name__)
app.config.update(
    SECRET_KEY=_secret_key(),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("RECON_HTTPS", "0") == "1",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 10,  # 10 hours
    MAX_CONTENT_LENGTH=int(os.environ.get("RECON_MAX_UPLOAD_MB", "100")) * 1024 * 1024,
)

if os.environ.get("RECON_BEHIND_PROXY", "0") == "1":
    # Trust X-Forwarded-For/Proto/Host/Prefix from ONE reverse proxy (nginx, IIS, F5).
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

RETENTION_DAYS = max(1, int(os.environ.get("RECON_RETENTION_DAYS", "7")))
ACTIVITY_DAYS = max(7, int(os.environ.get("RECON_ACTIVITY_DAYS", "90")))
AUDIT_DAYS = max(RETENTION_DAYS, int(os.environ.get("RECON_AUDIT_DAYS", "365")))
TS_FMT = "%Y-%m-%d %H:%M:%S"

APP_VERSION = "2026.11.05"
KINDS = {"compare", "stats", "dq", "code", "ddl", "json", "trend"}
MIN_PASSWORD = 8
QUERY_TIMEOUT = max(10, int(os.environ.get("RECON_QUERY_TIMEOUT", "300")))
QUERY_MAX_ROWS = max(100, int(os.environ.get("RECON_QUERY_MAX_ROWS", "200000")))
QUERY_SLOTS = threading.BoundedSemaphore(max(1, int(os.environ.get("RECON_QUERY_CONCURRENCY", "4"))))
DEMO_CONNECTIONS = os.environ.get("RECON_DEMO_CONNECTIONS", "0") == "1"

# Sign-in: "local" = Recon passwords only; "hue" = people sign in with their Hue / LDAP username and password
AUTH_MODE = os.environ.get("RECON_AUTH", "local").strip().lower()
HUE_CFG = {k: v for k, v in dict(
    url=os.environ.get("RECON_HUE_URL", "").strip(),
    api=os.environ.get("RECON_HUE_API", "auto").strip().lower(),
    login_server=os.environ.get("RECON_HUE_LOGIN_SERVER", "").strip(),
    verify_ssl="no" if os.environ.get("RECON_HUE_VERIFY_SSL", "1") == "0" else "yes",
    ca_bundle=os.environ.get("RECON_HUE_CA_BUNDLE", "").strip(),
    dialect="hive", database=os.environ.get("RECON_HUE_DATABASE", "").strip(),
).items() if v}
HUE_AUTO_CREATE = os.environ.get("RECON_HUE_AUTO_CREATE", "1") == "1"
# Incident / change number recorded with every query run (KPMG / audit evidence). 1 = must be entered before Run.
REQUIRE_INCIDENT = os.environ.get("RECON_REQUIRE_INCIDENT", "0") == "1"
INCIDENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_./ -]{2,39}$")
# Allow admins to run MSCK REPAIR TABLE from the Partitions tab (changes the Hive metastore; incident always required)
ALLOW_MSCK = os.environ.get("RECON_ALLOW_MSCK", "0") == "1"
SESSION_SECONDS = 60 * 60 * 10
if AUTH_MODE == "hue" and not HUE_CFG.get("url"):
    raise SystemExit("RECON_AUTH=hue needs RECON_HUE_URL, e.g. https://huehost.company.com:8888")


class CredVault:
    """Hue / LDAP passwords of signed-in people, in this server process's memory only.

    Encrypted with a random key created at start-up (never written anywhere), removed at sign-out
    or after the session lifetime. A server restart forgets everything: people simply sign in again.
    """

    def __init__(self):
        self._key = secrets.token_bytes(32)
        self._items = {}
        self._lock = threading.Lock()

    def _xor(self, nonce, data):
        out, i = bytearray(), 0
        while len(out) < len(data):
            out += hashlib.blake2b(nonce + i.to_bytes(4, "big"), key=self._key, digest_size=64).digest()
            i += 1
        return bytes(a ^ b for a, b in zip(data, out))

    def put(self, uid, username, password):
        vid, nonce = secrets.token_urlsafe(24), secrets.token_bytes(16)
        with self._lock:
            self._sweep()
            self._items[vid] = (uid, username, nonce, self._xor(nonce, password.encode()), time.time() + SESSION_SECONDS)
        return vid

    def get(self, vid, uid):
        with self._lock:
            item = self._items.get(vid or "")
            if not item or item[0] != uid or item[4] < time.time():
                return None
            return item[1], self._xor(item[2], item[3]).decode()

    def drop(self, vid=None, uid=None):
        with self._lock:
            for k in [k for k, v in self._items.items() if k == vid or (uid is not None and v[0] == uid)]:
                self._items.pop(k, None)

    def _sweep(self):
        t = time.time()
        for k in [k for k, v in self._items.items() if v[4] < t]:
            self._items.pop(k, None)


VAULT = CredVault()


# ---------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL COLLATE NOCASE,
  full_name TEXT NOT NULL DEFAULT '',
  password_hash TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1,
  must_change INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  last_login TEXT,
  auth_source TEXT NOT NULL DEFAULT 'local'
);
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id),
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  src_name TEXT,
  tgt_name TEXT,
  summary_json TEXT NOT NULL DEFAULT '{}',
  report_file TEXT,
  report_name TEXT,
  report_size INTEGER,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_user ON runs(user_id, created_at);
CREATE TABLE IF NOT EXISTS audit_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER,
  user_id INTEGER,
  username TEXT,
  full_name TEXT,
  kind TEXT NOT NULL,
  title TEXT,
  status TEXT,
  result TEXT,
  src_name TEXT, src_connection TEXT, src_type TEXT, src_sql TEXT, src_rows INTEGER, src_seconds REAL,
  tgt_name TEXT, tgt_connection TEXT, tgt_type TEXT, tgt_sql TEXT, tgt_rows INTEGER, tgt_seconds REAL,
  ran_at TEXT,
  created_at TEXT NOT NULL,
  ip TEXT,
  report_name TEXT,
  summary_json TEXT,
  incident TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_runs(created_at);
CREATE TABLE IF NOT EXISTS connections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL COLLATE NOCASE,
  type TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  config_json TEXT NOT NULL DEFAULT '{}',
  secret_mode TEXT NOT NULL DEFAULT 'ask',
  secret_ref TEXT NOT NULL DEFAULT '',
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS presets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id),
  name TEXT NOT NULL,
  data_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(user_id, name)
);
CREATE TABLE IF NOT EXISTS activity (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  username TEXT,
  action TEXT NOT NULL,
  detail TEXT,
  ip TEXT,
  at TEXT NOT NULL
);
"""


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    if "auth_source" not in [r[1] for r in conn.execute("PRAGMA table_info(users)")]:
        conn.execute("ALTER TABLE users ADD COLUMN auth_source TEXT NOT NULL DEFAULT 'local'")  # upgrade older databases
        conn.commit()
    if "incident" not in [r[1] for r in conn.execute("PRAGMA table_info(audit_runs)")]:
        conn.execute("ALTER TABLE audit_runs ADD COLUMN incident TEXT")
        conn.commit()
    has_user = conn.execute("SELECT 1 FROM users LIMIT 1").fetchone()
    if not has_user:
        username = os.environ.get("RECON_ADMIN_USER", "admin")
        password = os.environ.get("RECON_ADMIN_PASSWORD") or secrets.token_urlsafe(9)
        conn.execute(
            "INSERT INTO users(username, full_name, password_hash, is_admin, must_change, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (username, "Administrator", generate_password_hash(password), 1, 1, now()))
        conn.commit()
        print("=" * 60)
        print(f" First admin account created:  {username} / {password}")
        print(" You will be asked to change this password at first sign-in.")
        print("=" * 60)
    if DEMO_CONNECTIONS and not conn.execute("SELECT 1 FROM connections LIMIT 1").fetchone():
        _seed_demo(conn)
    if not DEMO_CONNECTIONS:  # remove the sample connections an older version may have created
        conn.execute("DELETE FROM connections WHERE type='sqlite' AND name IN ('Demo source (Hive-style)','Demo target (AWS-style)')")
        conn.commit()
    if HUE_CFG.get("url") and not conn.execute("SELECT 1 FROM connections WHERE type='hue' LIMIT 1").fetchone():
        cfg = {k: v for k, v in HUE_CFG.items() if k in ("url", "api", "login_server", "verify_ssl", "ca_bundle", "dialect", "database")}
        cfg["null_text"] = "yes"
        conn.execute("INSERT INTO connections(name, type, description, config_json, secret_mode, created_at)"
                     " VALUES (?,?,?,?,?,?)", ("Hive (Hue)", "hue", "Runs as the signed-in person through Hue",
                                              json.dumps(cfg), "signin" if AUTH_MODE == "hue" else "ask", now()))
        conn.commit()
    conn.close()


def _demo_monthly(target=False):
    """12 month-ends of balances for 6 accounts, with a spike, a missing month and a target mismatch."""
    import calendar
    months = [(2025 + (9 + i) // 12, (9 + i) % 12 + 1) for i in range(12)]  # 2025-10 .. 2026-09
    out = []
    for a in range(6):
        for i, (y, m) in enumerate(months):
            ead = 100000 + a * 25000 + i * 1800
            ecl = round(ead * (0.010 + a * 0.004) * (1 + i * 0.01), 2)
            if (y, m) == (2026, 6):
                ecl = round(ecl * 1.6, 2)          # stage migration spike in June
            prov = round(ecl * 1.05, 2)
            if target and (y, m) == (2026, 8) and a == 2:
                ecl = round(ecl * 0.93, 2)         # target lost part of August ECL for one account
            if target and (y, m) == (2026, 3) and a == 5:
                continue                           # one account missing in March on the target
            out.append((f"ACC10{a + 1:02d}", f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}", ead, ecl, prov))
    return out


def _seed_demo(conn):
    """Two small SQLite databases so the query flow can be tried without real systems."""
    rows_src = [("ACC1001", "Hs", "Stage 1", 125000.00, 1250.50, "Hyderabad", "2026-09-30"),
                ("ACC1002", "Ravi", "Stage 2", 98000.00, 6420.00, "Vizag", "2026-09-30"),
                ("ACC1003", "Anu", "Stage 1", 61000.00, 610.00, "Guntur", "2026-09-30"),
                ("ACC1004", "Kiran", "Stage 3", 45000.00, 22500.00, "Nellore", "2026-09-30"),
                ("ACC1005", "Sita", "Stage 1", 41000.00, 410.00, "Kadapa", "2026-09-30")]
    rows_tgt = [("ACC1001", "Hs", "Stage 1", 125000.00, 1250.50, "Hyderabad", "2026-09-30 00:00:00.000"),
                ("ACC1002", "Ravi", "Stage 2", 98000.00, 6240.00, "Vizag", "2026-09-30 00:00:00.000"),
                ("ACC1003", "Anu", "Stage 2", 61000.00, 610.00, "Guntur", "2026-09-30 00:00:00.000"),
                ("ACC1005", "Sita", "Stage 1", 41000.00, 410.00, "Kadapa", "2026-09-30 00:00:00.000"),
                ("ACC1006", "Mahesh", "Stage 1", 39000.00, 390.00, "Tirupati", "2026-09-30 00:00:00.000")]
    ddl = ("CREATE TABLE loan_impairments (account_id TEXT, customer_name TEXT, ifrs9_stage TEXT,"
           " ead NUMERIC, ecl NUMERIC, region TEXT, as_of_date TEXT)")
    for fname, rows in (("demo_source_hive.db", rows_src), ("demo_target_aws.db", rows_tgt)):
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            os.remove(path)
        d = sqlite3.connect(path)
        d.execute(ddl)
        d.executemany("INSERT INTO loan_impairments VALUES (?,?,?,?,?,?,?)", rows)
        d.execute("CREATE TABLE impairment_monthly (account_id TEXT, as_of_date TEXT, ead NUMERIC, ecl NUMERIC, provision NUMERIC)")
        d.executemany("INSERT INTO impairment_monthly VALUES (?,?,?,?,?)", _demo_monthly(target=fname.startswith("demo_target")))
        d.commit()
        d.close()
    for name, fname, desc in (("Demo source (Hive-style)", "demo_source_hive.db", "Sample data for trying the query flow"),
                              ("Demo target (AWS-style)", "demo_target_aws.db", "Sample data for trying the query flow")):
        conn.execute("INSERT INTO connections(name, type, description, config_json, secret_mode, created_at)"
                     " VALUES (?,?,?,?,?,?)", (name, "sqlite", desc, json.dumps({"path": fname}), "none", now()))
    conn.commit()


def log(action, detail="", user=None):
    u = user or getattr(g, "user", None)
    db().execute(
        "INSERT INTO activity(user_id, username, action, detail, ip, at) VALUES (?,?,?,?,?,?)",
        (u["id"] if u else None, u["username"] if u else None, action, detail[:500],
         request.remote_addr, now()))
    db().commit()


# ---------------------------------------------------------------- automatic clean-up
_purge_lock = threading.Lock()
_last_purge = [0.0]


def purge_old_runs():
    """Delete runs (and their Excel files) older than RETENTION_DAYS, and old activity rows."""
    with _purge_lock:
        _last_purge[0] = time.time()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime(TS_FMT)
        act_cutoff = (datetime.now(timezone.utc) - timedelta(days=ACTIVITY_DAYS)).strftime(TS_FMT)
        conn = sqlite3.connect(DB_PATH)
        try:
            old = conn.execute("SELECT id, report_file FROM runs WHERE created_at < ?", (cutoff,)).fetchall()
            for _rid, report in old:
                if report:
                    try:
                        os.remove(os.path.join(REPORT_DIR, report))
                    except FileNotFoundError:
                        pass
            conn.execute("DELETE FROM runs WHERE created_at < ?", (cutoff,))
            conn.execute("DELETE FROM activity WHERE at < ?", (act_cutoff,))
            audit_cutoff = (datetime.now(timezone.utc) - timedelta(days=AUDIT_DAYS)).strftime(TS_FMT)
            conn.execute("DELETE FROM audit_runs WHERE created_at < ?", (audit_cutoff,))
            if old:
                conn.execute("INSERT INTO activity(user_id, username, action, detail, ip, at) VALUES (NULL,'system','auto_cleanup',?,NULL,?)",
                             (f"Removed {len(old)} run(s) older than {RETENTION_DAYS} days", now()))
            conn.commit()
            return len(old)
        finally:
            conn.close()


def _purge_loop():
    while True:
        time.sleep(3600)
        try:
            purge_old_runs()
        except Exception as exc:  # keep the thread alive
            print("auto clean-up failed:", exc)


# ---------------------------------------------------------------- auth helpers
_failures = {}  # key -> [count, first_failure_time]
LOCK_AFTER, LOCK_SECONDS = 5, 300


def _locked(key):
    rec = _failures.get(key)
    if not rec:
        return False
    if time.time() - rec[1] > LOCK_SECONDS:
        _failures.pop(key, None)
        return False
    return rec[0] >= LOCK_AFTER


def _fail(key):
    rec = _failures.setdefault(key, [0, time.time()])
    rec[0] += 1


@app.before_request
def load_user():
    if time.time() - _last_purge[0] > 3600:
        try:
            purge_old_runs()
        except Exception as exc:
            print("auto clean-up failed:", exc)
    g.user = None
    uid = session.get("uid")
    if uid:
        row = db().execute("SELECT * FROM users WHERE id=? AND active=1", (uid,)).fetchone()
        if row:
            g.user = row
        else:
            session.clear()
    if "csrf" not in session:
        session["csrf"] = secrets.token_urlsafe(24)
    # CSRF check for every state-changing request
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        token = request.headers.get("X-CSRF-Token") or request.form.get("csrf")
        if not token or not secrets.compare_digest(token, session["csrf"]):
            abort(400, "Your session expired. Reload the page and try again.")


def login_required(fn):
    @wraps(fn)
    def wrapper(*a, **kw):
        if not g.user:
            if request.path.startswith("/api/"):
                return jsonify(error="Please sign in again."), 401
            return redirect(url_for("login"))
        if g.user["must_change"] and request.endpoint not in ("change_password", "me", "index", "logout"):
            return jsonify(error="Change your password first."), 403
        return fn(*a, **kw)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    @login_required
    def wrapper(*a, **kw):
        if not g.user["is_admin"]:
            return jsonify(error="Only admins can do this."), 403
        return fn(*a, **kw)
    return wrapper


def user_dict(row):
    return dict(id=row["id"], username=row["username"], full_name=row["full_name"],
                is_admin=bool(row["is_admin"]), active=bool(row["active"]),
                must_change=bool(row["must_change"]), created_at=row["created_at"],
                last_login=row["last_login"], auth_source=row["auth_source"])


# ---------------------------------------------------------------- pages
@app.get("/healthz")
def healthz():
    """For load balancers and monitoring: checks the database answers."""
    try:
        db().execute("SELECT 1").fetchone()
        return jsonify(status="ok", version=APP_VERSION), 200
    except Exception as exc:  # pragma: no cover
        return jsonify(status="error", detail=str(exc)), 503


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        key = (username.lower(), request.remote_addr)
        if _locked(key):
            error = "Too many failed attempts. Wait 5 minutes and try again."
        else:
            row = db().execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
            via = None
            if row and row["active"] and row["auth_source"] != "hue" and check_password_hash(row["password_hash"], password):
                via = "local"
            elif (AUTH_MODE == "hue" and username and password and (not row or row["active"])
                  and (not row or row["auth_source"] == "hue" or not row["is_admin"])):
                # Local admins never sign in through Hue (keeps a break-glass account); everyone else can.
                try:
                    C.hue_check_login(HUE_CFG, username, password)
                    via = "hue"
                except C.QueryError as exc:
                    if "sign-in failed" not in str(exc) and "refused" not in str(exc):
                        error = "Could not check your password with Hue: " + str(exc)
                if via == "hue":
                    if not row:
                        if not HUE_AUTO_CREATE or not all(c.isalnum() or c in "._-@" for c in username) or len(username) > 60:
                            via, error = None, "You don't have a Recon account yet. Ask your Recon admin to add you."
                        else:
                            db().execute("INSERT INTO users(username, full_name, password_hash, is_admin, must_change,"
                                         " created_at, auth_source) VALUES (?,?,?,?,?,?,?)",
                                         (username, username, "!hue", 0, 0, now(), "hue"))
                            db().commit()
                            row = db().execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
                            log("user_created", f"{username} (first Hue sign-in)", user=row)
                    elif row["auth_source"] != "hue":
                        db().execute("UPDATE users SET auth_source='hue', must_change=0, password_hash='!hue' WHERE id=?", (row["id"],))
                        db().commit()
            if via:
                _failures.pop(key, None)
                session.clear()
                session.permanent = True
                session["uid"] = row["id"]
                session["csrf"] = secrets.token_urlsafe(24)
                if via == "hue":
                    session["vid"] = VAULT.put(row["id"], row["username"], password)
                db().execute("UPDATE users SET last_login=? WHERE id=?", (now(), row["id"]))
                db().commit()
                log("login", "via Hue" if via == "hue" else "", user=row)
                return redirect(url_for("index"))
            if not error:
                _fail(key)
                error = ("This account is disabled. Ask an admin to enable it."
                         if row and not row["active"] else "Username or password is wrong.")
    return render_template("login.html", error=error, csrf=session["csrf"], auth_mode=AUTH_MODE)


@app.post("/logout")
def logout():
    if g.user:
        log("logout")
    VAULT.drop(vid=session.get("vid"))
    session.clear()
    return redirect(url_for("login"))


@app.get("/")
@login_required
def index():
    return render_template("index.html", csrf=session["csrf"], me=user_dict(g.user), version=APP_VERSION)


# ---------------------------------------------------------------- account
@app.get("/api/me")
@login_required
def me():
    return jsonify(user_dict(g.user))


@app.post("/api/me/password")
@login_required
def change_password():
    body = request.get_json(force=True, silent=True) or {}
    cur, new = body.get("current", ""), body.get("new", "")
    if g.user["auth_source"] == "hue":
        return jsonify(error="You sign in with your Hue / LDAP password. Change it the usual company way."), 400
    if not check_password_hash(g.user["password_hash"], cur):
        return jsonify(error="Your current password is wrong."), 400
    if len(new) < MIN_PASSWORD:
        return jsonify(error=f"Use at least {MIN_PASSWORD} characters."), 400
    db().execute("UPDATE users SET password_hash=?, must_change=0 WHERE id=?",
                 (generate_password_hash(new), g.user["id"]))
    db().commit()
    log("password_changed")
    return jsonify(ok=True)


# ---------------------------------------------------------------- runs + reports
RUN_SELECT = ("SELECT r.*, u.username, u.full_name FROM runs r JOIN users u ON u.id=r.user_id")


def run_dict(row):
    created = datetime.strptime(row["created_at"], TS_FMT)
    return dict(id=row["id"], kind=row["kind"], title=row["title"], src_name=row["src_name"],
                tgt_name=row["tgt_name"], summary=json.loads(row["summary_json"] or "{}"),
                has_report=bool(row["report_file"]), report_name=row["report_name"],
                report_size=row["report_size"], created_at=row["created_at"],
                expires_at=(created + timedelta(days=RETENTION_DAYS)).strftime(TS_FMT),
                user_id=row["user_id"], username=row["username"],
                full_name=row["full_name"] or row["username"],
                can_delete=bool(row["user_id"] == g.user["id"] or g.user["is_admin"]))


@app.get("/api/runs")
@login_required
def list_runs():
    """Team history: every signed-in user sees everyone's runs from the last RETENTION_DAYS days."""
    limit = min(int(request.args.get("limit", 300)), 1000)
    kind = request.args.get("kind")
    q, args = RUN_SELECT + " WHERE 1=1", []
    if request.args.get("who") == "mine":
        q += " AND r.user_id=?"
        args.append(g.user["id"])
    if kind in KINDS:
        q += " AND r.kind=?"
        args.append(kind)
    search = request.args.get("q", "").strip()[:100]
    if search:
        like = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        q += (" AND (r.title LIKE ? ESCAPE '\\' OR r.src_name LIKE ? ESCAPE '\\' OR r.tgt_name LIKE ? ESCAPE '\\'"
              " OR u.username LIKE ? ESCAPE '\\' OR u.full_name LIKE ? ESCAPE '\\')")
        args += [like] * 5
    q += " ORDER BY r.created_at DESC, r.id DESC LIMIT ?"
    args.append(limit)
    rows = db().execute(q, args).fetchall()
    return jsonify(runs=[run_dict(r) for r in rows], retention_days=RETENTION_DAYS)


@app.post("/api/runs")
@login_required
def create_run():
    meta = json.loads(request.form.get("meta", "{}"))
    kind = meta.get("kind")
    if kind not in KINDS:
        return jsonify(error="Unknown run type."), 400
    summary = json.dumps(meta.get("summary", {}))
    if len(summary) > 2_000_000:
        return jsonify(error="Run summary is too large to save."), 400
    report_file = report_name = report_size = None
    f = request.files.get("report")
    if f:
        if not f.filename.lower().endswith(".xlsx"):
            return jsonify(error="Reports must be .xlsx files."), 400
        user_dir = os.path.join(REPORT_DIR, str(g.user["id"]))
        os.makedirs(user_dir, exist_ok=True)
        report_file = f"{g.user['id']}/{uuid.uuid4().hex}.xlsx"
        path = os.path.join(REPORT_DIR, report_file)
        f.save(path)
        report_name = os.path.basename(f.filename)[:200]
        report_size = os.path.getsize(path)
    cur = db().execute(
        "INSERT INTO runs(user_id, kind, title, src_name, tgt_name, summary_json, report_file,"
        " report_name, report_size, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (g.user["id"], kind, str(meta.get("title", kind))[:200], str(meta.get("src_name", ""))[:300],
         str(meta.get("tgt_name", ""))[:300], summary, report_file, report_name, report_size, now()))
    db().commit()
    _write_audit(cur.lastrowid, kind, meta, report_name)
    log("run_" + kind, meta.get("title", ""))
    row = db().execute(RUN_SELECT + " WHERE r.id=?", (cur.lastrowid,)).fetchone()
    return jsonify(run_dict(row)), 201


def _write_audit(run_id, kind, meta, report_name):
    """Keep a long-lived audit record of every run: who, when, which systems, which queries, what result."""
    sm = meta.get("summary") or {}
    q = sm.get("query") or {}
    src, tgt = q.get("source") or {}, q.get("target") or {}
    clip = lambda v, n=20000: (str(v)[:n] if v is not None else None)  # noqa: E731
    if not q and kind in ("code", "ddl", "json"):
        src = {"sql": sm.get("source_code")}
        tgt = {"sql": sm.get("target_code")}
    try:
        db().execute(
            "INSERT INTO audit_runs(run_id, user_id, username, full_name, kind, title, status, result,"
            " src_name, src_connection, src_type, src_sql, src_rows, src_seconds,"
            " tgt_name, tgt_connection, tgt_type, tgt_sql, tgt_rows, tgt_seconds,"
            " ran_at, created_at, ip, report_name, summary_json, incident) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, g.user["id"], g.user["username"], g.user["full_name"] or g.user["username"], kind,
             clip(meta.get("title"), 300), clip(sm.get("status"), 20), clip(sm.get("text"), 1000),
             clip(meta.get("src_name"), 300), clip(src.get("connection"), 200), clip(src.get("type"), 40),
             clip(src.get("sql")), src.get("rows"), src.get("seconds"),
             clip(meta.get("tgt_name"), 300), clip(tgt.get("connection"), 200), clip(tgt.get("type"), 40),
             clip(tgt.get("sql")), tgt.get("rows"), tgt.get("seconds"),
             clip(q.get("ran_at"), 40), now(), request.remote_addr, report_name,
             json.dumps({k: v for k, v in sm.items() if k not in ("source_code", "target_code")})[:200000],
             clip(q.get("incident"), 40)))
        db().commit()
    except Exception as exc:  # the audit must never block the user's run
        print("audit write failed:", exc)


def _audit_event(kind, title, status, result, connection, conn_type, sql, incident):
    """Audit record for an action that is not a compare run (AWS console opened, MSCK REPAIR)."""
    try:
        db().execute(
            "INSERT INTO audit_runs(user_id, username, full_name, kind, title, status, result, src_name, src_connection,"
            " src_type, src_sql, ran_at, created_at, ip, incident) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (g.user["id"], g.user["username"], g.user["full_name"] or g.user["username"], kind, title[:300], status, result[:1000],
             connection, connection, conn_type, (sql or "")[:20000], now(), now(), request.remote_addr, incident))
        db().commit()
    except Exception as exc:
        print("audit write failed:", exc)


def _incident(body, required=None):
    inc = " ".join(str(body.get("incident", "") or "").split())[:40]
    need = REQUIRE_INCIDENT if required is None else required
    if not inc and need:
        raise C.QueryError("Enter the incident / change number first (e.g. INC1234567 or CHG0012345).")
    if inc and not INCIDENT_RE.match(inc):
        raise C.QueryError("The incident / change number can use letters, digits, - _ . / and spaces (3 to 40 characters).")
    return inc


@app.get("/api/audit")
@login_required
def audit_list():
    """Audit report: every run in a date range with systems, queries, timestamps and results."""
    q, args = ("SELECT a.*, r.id AS live_run, r.report_file AS live_report FROM audit_runs a"
               " LEFT JOIN runs r ON r.id = a.run_id WHERE 1=1"), []
    for key, op in (("from", ">="), ("to", "<")):
        v = request.args.get(key, "").strip()
        if v:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?", v):
                return jsonify(error="Dates must look like 2026-10-23."), 400
            q += f" AND a.created_at {op} ?"
            args.append(v if len(v) > 10 else v + " 00:00:00")
    for key, col in (("user", "a.username"), ("kind", "a.kind"), ("status", "a.status")):
        v = request.args.get(key, "").strip()
        if v:
            q += f" AND {col} = ?"
            args.append(v)
    search = request.args.get("q", "").strip()[:100]
    if search:
        like = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        q += (" AND (a.title LIKE ? ESCAPE '\\' OR a.src_sql LIKE ? ESCAPE '\\' OR a.tgt_sql LIKE ? ESCAPE '\\'"
              " OR a.src_connection LIKE ? ESCAPE '\\' OR a.tgt_connection LIKE ? ESCAPE '\\' OR a.full_name LIKE ? ESCAPE '\\'"
              " OR a.incident LIKE ? ESCAPE '\\')")
        args += [like] * 7
    q += " ORDER BY a.created_at DESC, a.id DESC LIMIT 5000"
    rows = []
    for r in db().execute(q, args).fetchall():
        d = {k: r[k] for k in r.keys() if k not in ("summary_json", "live_report", "live_run")}
        d["summary"] = json.loads(r["summary_json"] or "{}")
        d["report_available"] = bool(r["live_report"])
        rows.append(d)
    users = [dict(username=u["username"], full_name=u["full_name"] or u["username"])
             for u in db().execute("SELECT username, full_name FROM users ORDER BY full_name, username").fetchall()]
    return jsonify(rows=rows, users=users, audit_days=AUDIT_DAYS, report_days=RETENTION_DAYS)


def _team_run(run_id):
    """Any signed-in team member can open and download any run in team history."""
    row = db().execute(RUN_SELECT + " WHERE r.id=?", (run_id,)).fetchone()
    if not row:
        abort(404, "This run no longer exists. Runs are removed after %d days." % RETENTION_DAYS)
    return row


@app.get("/api/runs/<int:run_id>")
@login_required
def get_run(run_id):
    row = _team_run(run_id)
    return jsonify(run_dict(row))


@app.get("/api/runs/<int:run_id>/report")
@login_required
def get_report(run_id):
    row = _team_run(run_id)
    if not row["report_file"]:
        abort(404)
    path = os.path.realpath(os.path.join(REPORT_DIR, row["report_file"]))
    if not path.startswith(os.path.realpath(REPORT_DIR)) or not os.path.exists(path):
        abort(404)
    if row["user_id"] != g.user["id"]:
        log("report_downloaded", f"{row['report_name']} (run by {row['username']})")
    return send_file(path, as_attachment=True, download_name=row["report_name"] or "report.xlsx")


@app.delete("/api/runs/<int:run_id>")
@login_required
def delete_run(run_id):
    row = _team_run(run_id)
    if row["user_id"] != g.user["id"] and not g.user["is_admin"]:
        return jsonify(error="Only the person who ran it, or an admin, can delete this run."), 403
    if row["report_file"]:
        try:
            os.remove(os.path.join(REPORT_DIR, row["report_file"]))
        except FileNotFoundError:
            pass
    db().execute("DELETE FROM runs WHERE id=?", (run_id,))
    db().commit()
    log("run_deleted", str(run_id))
    return jsonify(ok=True)


# ---------------------------------------------------------------- presets
@app.get("/api/presets")
@login_required
def list_presets():
    rows = db().execute("SELECT * FROM presets WHERE user_id=? ORDER BY name", (g.user["id"],)).fetchall()
    return jsonify([dict(id=r["id"], name=r["name"], data=json.loads(r["data_json"]),
                         created_at=r["created_at"]) for r in rows])


@app.post("/api/presets")
@login_required
def save_preset():
    body = request.get_json(force=True, silent=True) or {}
    name = str(body.get("name", "")).strip()[:80]
    if not name:
        return jsonify(error="Give the preset a name."), 400
    data = json.dumps(body.get("data", {}))
    if len(data) > 100_000:
        return jsonify(error="Preset is too large."), 400
    db().execute(
        "INSERT INTO presets(user_id, name, data_json, created_at) VALUES (?,?,?,?)"
        " ON CONFLICT(user_id, name) DO UPDATE SET data_json=excluded.data_json, created_at=excluded.created_at",
        (g.user["id"], name, data, now()))
    db().commit()
    return jsonify(ok=True)


@app.delete("/api/presets/<int:pid>")
@login_required
def delete_preset(pid):
    db().execute("DELETE FROM presets WHERE id=? AND user_id=?", (pid, g.user["id"]))
    db().commit()
    return jsonify(ok=True)


# ---------------------------------------------------------------- admin
@app.get("/api/admin/users")
@admin_required
def admin_users():
    rows = db().execute(
        "SELECT u.*, (SELECT COUNT(*) FROM runs r WHERE r.user_id=u.id) AS run_count"
        " FROM users u ORDER BY u.username").fetchall()
    out = []
    for r in rows:
        d = user_dict(r)
        d["run_count"] = r["run_count"]
        out.append(d)
    return jsonify(out)


@app.post("/api/admin/users")
@admin_required
def admin_create_user():
    body = request.get_json(force=True, silent=True) or {}
    username = str(body.get("username", "")).strip()
    if not username or len(username) > 60 or not all(c.isalnum() or c in "._-@" for c in username):
        return jsonify(error="Usernames use letters, numbers and . _ - @ only."), 400
    password = body.get("password") or secrets.token_urlsafe(9)
    if len(password) < MIN_PASSWORD:
        return jsonify(error=f"Use at least {MIN_PASSWORD} characters."), 400
    try:
        db().execute(
            "INSERT INTO users(username, full_name, password_hash, is_admin, must_change, created_at)"
            " VALUES (?,?,?,?,1,?)",
            (username, str(body.get("full_name", ""))[:100], generate_password_hash(password),
             1 if body.get("is_admin") else 0, now()))
        db().commit()
    except sqlite3.IntegrityError:
        return jsonify(error="That username is already taken."), 400
    log("user_created", username)
    return jsonify(ok=True, username=username, temp_password=password), 201


@app.patch("/api/admin/users/<int:uid>")
@admin_required
def admin_update_user(uid):
    body = request.get_json(force=True, silent=True) or {}
    row = db().execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not row:
        abort(404)
    out = {"ok": True}
    if uid == g.user["id"] and (body.get("active") is False or body.get("is_admin") is False):
        return jsonify(error="You can't disable or demote your own account."), 400
    if "active" in body:
        db().execute("UPDATE users SET active=? WHERE id=?", (1 if body["active"] else 0, uid))
        if not body["active"]:
            VAULT.drop(uid=uid)
        log("user_" + ("enabled" if body["active"] else "disabled"), row["username"])
    if "is_admin" in body:
        db().execute("UPDATE users SET is_admin=? WHERE id=?", (1 if body["is_admin"] else 0, uid))
        log("user_admin_" + ("on" if body["is_admin"] else "off"), row["username"])
    if "full_name" in body:
        db().execute("UPDATE users SET full_name=? WHERE id=?", (str(body["full_name"])[:100], uid))
    if body.get("reset_password"):
        temp = secrets.token_urlsafe(9)
        db().execute("UPDATE users SET password_hash=?, must_change=1 WHERE id=?",
                     (generate_password_hash(temp), uid))
        log("password_reset", row["username"])
        out["temp_password"] = temp
    db().commit()
    return jsonify(out)


# ---------------------------------------------------------------- connections + queries
def _signin_ready():
    """True when this person signed in to Recon with their Hue / LDAP password (so 'signin' connections need no password box)."""
    u = getattr(g, "user", None)
    return bool(AUTH_MODE == "hue" and u is not None and u["auth_source"] == "hue" and VAULT.get(session.get("vid"), u["id"]))


def conn_public(row, admin=False):
    t = C.TYPES.get(row["type"], {})
    cfg = json.loads(row["config_json"] or "{}")
    d = dict(id=row["id"], name=row["name"], type=row["type"], type_label=t.get("label", row["type"]),
             icon=t.get("icon", "db"), description=row["description"], secret_mode=row["secret_mode"],
             ask_fields=t.get("ask_fields", []) if row["secret_mode"] == "ask" or (row["secret_mode"] == "signin" and not _signin_ready()) else [],
             sql_hint=t.get("sql_hint", ""), active=bool(row["active"]),
             summary=_conn_summary(row["type"], cfg), dialect=cfg.get("dialect", ""),
             console=row["type"] == "athena" and cfg.get("console_link") == "yes" and bool(cfg.get("role_arn")))
    if admin:
        d.update(config=cfg, secret_ref=row["secret_ref"], created_at=row["created_at"], updated_at=row["updated_at"])
    return d


def _conn_summary(kind, cfg):
    if kind == "athena":
        return f"{cfg.get('region', '')} · {cfg.get('database', '')} · workgroup {cfg.get('workgroup') or 'primary'}"
    if kind == "sqlite":
        return "demo · " + cfg.get("path", "")
    if kind == "hue":
        return f"{cfg.get('url', '')} · {cfg.get('dialect') or 'hive'}" + (f" / {cfg['database']}" if cfg.get("database") else "")
    if kind == "oracle" and cfg.get("dsn"):
        return "custom DSN"
    host = cfg.get("host") or cfg.get("cluster_identifier") or cfg.get("serverless_workgroup") or ""
    port = f":{cfg['port']}" if cfg.get("port") and cfg.get("host") else ""
    db_ = cfg.get("database") or cfg.get("service_name") or ""
    return f"{host}{port}" + (f" / {db_}" if db_ else "")


def _get_conn(conn_id):
    row = db().execute("SELECT * FROM connections WHERE id=? AND active=1", (conn_id,)).fetchone()
    if not row:
        raise C.QueryError("That connection no longer exists. Pick another one.")
    return row


def resolve_credentials(row, supplied):
    """Credentials for one run. Nothing here is ever written to the database or the logs."""
    mode, ref = row["secret_mode"], (row["secret_ref"] or "").strip()
    cfg = json.loads(row["config_json"] or "{}")
    if mode == "none":
        return {}
    if mode == "signin":
        if _signin_ready():
            cred = VAULT.get(session.get("vid"), g.user["id"])
            return {"username": cred[0], "password": cred[1]}
        # Not signed in with Hue / LDAP (e.g. local admin, or LDAP sign-in switched off): ask like "each user" mode.
        sup = supplied or {}
        user, pw = str(sup.get("username", "")).strip(), str(sup.get("password", ""))
        if not (user and pw):
            raise C.QueryError(f"Enter your Hue / LDAP username and password for {row['name']}.")
        return {"username": user, "password": pw}
    if mode == "ask":
        fields = C.TYPES[row["type"]]["ask_fields"]
        cred = {f["key"]: str((supplied or {}).get(f["key"], "")).strip() for f in fields}
        missing = [f["label"] for f in fields if not f.get("optional") and not cred[f["key"]]
                   and not (f["key"] == "username" and cfg.get("username"))]
        if missing:
            raise C.QueryError(f"Enter your {' and '.join(m.lower() for m in missing)} for {row['name']}.")
        return cred
    if mode == "env":
        if not ref or ref not in os.environ:
            raise C.QueryError(f"The server secret for {row['name']} is not set. Ask your Recon admin.")
        return {"password": os.environ[ref], "username": os.environ.get(ref + "_USER", "")}
    if mode == "aws_secret":
        boto3 = C._need("boto3", "boto3")
        region = cfg.get("region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "eu-west-1"
        try:
            value = boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId=ref)["SecretString"]
        except Exception as exc:
            raise C.QueryError(f"Could not read the AWS secret for {row['name']}: {str(exc)[:200]}")
        try:
            data = json.loads(value)
        except ValueError:
            return {"password": value}
        return {"username": data.get("username") or data.get("user") or "",
                "password": data.get("password") or data.get("pass") or "",
                "aws_access_key_id": data.get("aws_access_key_id", ""),
                "aws_secret_access_key": data.get("aws_secret_access_key", ""),
                "aws_session_token": data.get("aws_session_token", "")}
    raise C.QueryError("Unknown secret mode.")


_ACTIVE_RUNS = {}
_ACTIVE_LOCK = threading.Lock()


def _run_side(row, cred, sql, max_rows, ctx, dates):
    if not QUERY_SLOTS.acquire(timeout=30):
        raise C.QueryError("The server is busy running other queries. Try again in a minute.")
    try:
        return C.run_query(row["type"], json.loads(row["config_json"] or "{}"), cred, sql, max_rows, ctx,
                           dates=dates, data_dir=DATA_DIR)
    finally:
        QUERY_SLOTS.release()


@app.get("/api/connections")
@login_required
def list_connections():
    rows = db().execute("SELECT * FROM connections WHERE active=1 ORDER BY type, name").fetchall()
    return jsonify(connections=[conn_public(r) for r in rows], max_rows=QUERY_MAX_ROWS, timeout=QUERY_TIMEOUT,
                   require_incident=REQUIRE_INCIDENT, allow_msck=ALLOW_MSCK and bool(g.user["is_admin"]))


def _hive_row(body):
    row = _get_conn(int(body.get("connection_id") or 0))
    cfg = json.loads(row["config_json"] or "{}")
    if row["type"] == "sqlite":
        cfg["_data_dir"] = DATA_DIR
    return row, cfg, resolve_credentials(row, body.get("credentials"))


@app.post("/api/hive/partitions")
@login_required
def hive_partitions():
    body = request.get_json(force=True, silent=True) or {}
    try:
        row, cfg, cred = _hive_row(body)
        with QUERY_SLOTS:
            out = C.list_partitions(row["type"], cfg, cred, body.get("table", ""), C.RunContext(QUERY_TIMEOUT))
    except (C.QueryError, ValueError) as exc:
        return jsonify(error=str(exc)), 400
    log("partitions_listed", f"{row['name']}: {out['table']} ({len(out['partitions'])} partitions)")
    return jsonify(out)


@app.post("/api/hive/browse")
@login_required
def hive_browse():
    body = request.get_json(force=True, silent=True) or {}
    try:
        row, cfg, cred = _hive_row(body)
        if row["type"] != "hue":
            raise C.QueryError("Folder browsing works through a 'Hive via Hue' connection.")
        out = C.hue_list_dir(cfg, cred, body.get("path", ""), C.RunContext(min(QUERY_TIMEOUT, 120)))
    except (C.QueryError, ValueError) as exc:
        return jsonify(error=str(exc)), 400
    log("hdfs_browsed", f"{row['name']}: {out['path']} ({len(out['entries'])} items)")
    return jsonify(out)


@app.post("/api/hive/msck")
@admin_required
def hive_msck():
    if not ALLOW_MSCK:
        return jsonify(error="MSCK REPAIR is switched off. An admin can switch it on with RECON_ALLOW_MSCK=1."), 403
    body = request.get_json(force=True, silent=True) or {}
    try:
        incident = _incident(body, required=True)
    except C.QueryError as exc:
        return jsonify(error=str(exc)), 400
    try:
        row, cfg, cred = _hive_row(body)
        with QUERY_SLOTS:
            sql = C.run_msck(row["type"], cfg, cred, body.get("table", ""), C.RunContext(QUERY_TIMEOUT))
    except (C.QueryError, ValueError) as exc:
        _audit_event("msck", f"MSCK REPAIR · {body.get('table', '')}", "FAIL", str(exc), None, None,
                     f"MSCK REPAIR TABLE {body.get('table', '')}", incident)
        return jsonify(error=str(exc)), 400
    except Exception as exc:
        return jsonify(error="MSCK REPAIR failed: " + str(exc).splitlines()[0][:300]), 400
    log("msck_repair", f"[{incident}] {row['name']}: {sql}")
    _audit_event("msck", f"MSCK REPAIR · {body.get('table', '')}", "PASS", "MSCK REPAIR finished", row["name"], row["type"], sql, incident)
    return jsonify(ok=True, sql=sql)


@app.post("/api/connections/<int:conn_id>/console")
@login_required
def open_console(conn_id):
    """One-click sign-in to the AWS Athena console with the connection's IAM role. Recorded in the audit report."""
    body = request.get_json(force=True, silent=True) or {}
    try:
        row = _get_conn(conn_id)
        cfg = json.loads(row["config_json"] or "{}")
        if row["type"] != "athena" or cfg.get("console_link") != "yes":
            raise C.QueryError("The AWS console button is not switched on for this connection.")
        incident = _incident(body)
        url = C.aws_console_url(cfg, resolve_credentials(row, body.get("credentials")), g.user["username"])
    except C.QueryError as exc:
        return jsonify(error=str(exc)), 400
    log("aws_console_opened", (f"[{incident}] " if incident else "") + f"{row['name']} as {cfg.get('role_arn')}")
    _audit_event("console", f"AWS Athena console · {row['name']}", "", f"Signed in to the AWS console with {cfg.get('role_arn')}",
                 row["name"], "athena", body.get("sql") or "", incident or None)
    return jsonify(url=url)


@app.post("/api/connections/<int:conn_id>/test")
@login_required
def test_connection(conn_id):
    body = request.get_json(force=True, silent=True) or {}
    try:
        row = _get_conn(conn_id)
        cred = resolve_credentials(row, body.get("credentials"))
        ctx = C.RunContext(min(60, QUERY_TIMEOUT))
        out = _run_side(row, cred, C.TYPES[row["type"]]["test_sql"], 5, ctx, True)
        log("connection_tested", row["name"])
        return jsonify(ok=True, seconds=out["seconds"])
    except C.QueryError as exc:
        return jsonify(ok=False, error=str(exc)), 200


@app.post("/api/query/run")
@login_required
def query_run():
    """Run the source and target queries at the same time and return both result sets."""
    body = request.get_json(force=True, silent=True) or {}
    run_id = str(body.get("run_id", ""))[:64]
    dates = bool((body.get("options") or {}).get("normalize_dates", True))
    max_rows = min(QUERY_MAX_ROWS, int((body.get("options") or {}).get("max_rows") or QUERY_MAX_ROWS))
    sides, prepared, out = ("source", "target"), {}, {}
    if not any((body.get(sd) or {}).get("sql") for sd in sides):
        return jsonify(error="Enter at least one query."), 400
    try:
        incident = _incident(body)
    except C.QueryError as exc:
        return jsonify(error=str(exc)), 400
    for side in sides:
        spec = body.get(side) or {}
        if not spec.get("sql") and not spec.get("connection_id"):
            continue  # this side was left empty on purpose (single-side Statistics / Data quality)
        try:
            row = _get_conn(int(spec.get("connection_id") or 0))
            C.check_read_only(spec.get("sql", ""))
            prepared[side] = (row, resolve_credentials(row, spec.get("credentials")), spec["sql"])
        except (C.QueryError, ValueError) as exc:
            out[side] = dict(error=str(exc))
    contexts = {side: C.RunContext(QUERY_TIMEOUT) for side in prepared}
    key = (g.user["id"], run_id)
    with _ACTIVE_LOCK:
        _ACTIVE_RUNS[key] = list(contexts.values())
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futs = {side: pool.submit(_run_side, row, cred, sql, max_rows, contexts[side], dates)
                    for side, (row, cred, sql) in prepared.items()}
            for side, fut in futs.items():
                row = prepared[side][0]
                try:
                    res = fut.result(timeout=QUERY_TIMEOUT + 30)
                    res.update(connection=dict(id=row["id"], name=row["name"], type=row["type"],
                                               type_label=C.TYPES[row["type"]]["label"]),
                               row_count=len(res["rows"]), max_rows=max_rows)
                    out[side] = res
                except concurrent.futures.TimeoutError:
                    contexts[side].cancel()
                    out[side] = dict(error=f"Timed out after {QUERY_TIMEOUT} seconds.")
                except C.QueryError as exc:
                    out[side] = dict(error=str(exc))
    finally:
        with _ACTIVE_LOCK:
            _ACTIVE_RUNS.pop(key, None)
    parts = []
    for side in sides:
        spec = body.get(side) or {}
        if side not in out and side not in prepared:
            continue
        name = prepared[side][0]["name"] if side in prepared else f"connection {spec.get('connection_id')}"
        res = out.get(side, {})
        state = f"{res.get('row_count')} rows" if "rows" in res else "error: " + res.get("error", "")[:80]
        parts.append(f"{side} {name} ({state}): {' '.join(str(spec.get('sql', '')).split())[:160]}")
    log("query_run", (f"[{incident}] " if incident else "") + " | ".join(parts))
    return jsonify(out)


@app.post("/api/ddl/fetch")
@login_required
def ddl_fetch():
    """Fetch CREATE TABLE text for the named tables on one or both sides."""
    body = request.get_json(force=True, silent=True) or {}
    out, jobs = {}, {}
    for side in ("source", "target"):
        spec = body.get(side) or {}
        if not spec.get("connection_id") or not str(spec.get("tables", "")).strip():
            continue
        try:
            row = _get_conn(int(spec["connection_id"]))
            C.parse_table_list(spec["tables"])
            jobs[side] = (row, resolve_credentials(row, spec.get("credentials")), spec["tables"])
        except (C.QueryError, ValueError) as exc:
            out[side] = dict(error=str(exc))
    if not jobs and not out:
        return jsonify(error="Pick a connection and enter table names."), 400

    def work(row, cred, tables):
        if not QUERY_SLOTS.acquire(timeout=30):
            raise C.QueryError("The server is busy running other queries. Try again in a minute.")
        try:
            return C.fetch_ddl(row["type"], json.loads(row["config_json"] or "{}"), cred, tables,
                               C.RunContext(min(QUERY_TIMEOUT, 120)), data_dir=DATA_DIR)
        finally:
            QUERY_SLOTS.release()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = {side: pool.submit(work, *job) for side, job in jobs.items()}
        for side, fut in futs.items():
            try:
                res = fut.result(timeout=QUERY_TIMEOUT + 30)
                res["connection"] = dict(id=jobs[side][0]["id"], name=jobs[side][0]["name"])
                out[side] = res
            except (C.QueryError, concurrent.futures.TimeoutError) as exc:
                out[side] = dict(error=str(exc) or "Timed out.")
    log("ddl_fetch", " | ".join(f"{side} {jobs[side][0]['name']}: {jobs[side][2][:120]}" for side in jobs))
    return jsonify(out)


@app.post("/api/query/cancel")
@login_required
def query_cancel():
    run_id = str((request.get_json(force=True, silent=True) or {}).get("run_id", ""))[:64]
    with _ACTIVE_LOCK:
        ctxs = _ACTIVE_RUNS.get((g.user["id"], run_id), [])
    for ctx in ctxs:
        ctx.cancel()
    return jsonify(ok=True, cancelled=len(ctxs))


@app.get("/api/admin/connections")
@admin_required
def admin_connections():
    rows = db().execute("SELECT * FROM connections ORDER BY type, name").fetchall()
    return jsonify(connections=[conn_public(r, admin=True) for r in rows], types=C.public_type_info(),
                   secret_modes=C.SECRET_MODE_LABEL)


def _validate_conn_body(body):
    kind = body.get("type")
    if kind not in C.TYPES:
        raise C.QueryError("Pick a connection type.")
    t = C.TYPES[kind]
    name = str(body.get("name", "")).strip()[:80]
    if not name:
        raise C.QueryError("Give the connection a name.")
    mode = body.get("secret_mode") or t["secret_modes"][0]
    if mode not in t["secret_modes"]:
        raise C.QueryError(f"{t['label']} connections can't use that credential option.")
    ref = str(body.get("secret_ref", "")).strip()[:300]
    if mode in ("env", "aws_secret") and not ref:
        raise C.QueryError("Enter the environment variable name or AWS secret ID.")
    if mode == "env" and not re.fullmatch(r"[A-Z][A-Z0-9_]{2,80}", ref):
        raise C.QueryError("Environment variable names use capital letters, digits and _ (e.g. RECON_ORACLE_PROD_PASSWORD).")
    cfg_in = body.get("config") or {}
    cfg = {}
    for f in t["fields"]:
        v = str(cfg_in.get(f["key"], f.get("default", "")) or "").strip()[:2000]
        if f.get("choices") and v and v not in f["choices"]:
            raise C.QueryError(f"{f['label']}: choose one of {', '.join(f['choices'])}.")
        if f.get("required") and not v:
            raise C.QueryError(f"{f['label']} is required.")
        if v:
            cfg[f["key"]] = v
    if kind == "oracle" and not (cfg.get("dsn") or (cfg.get("host") and cfg.get("service_name"))):
        raise C.QueryError("Enter host and service name, or a full DSN.")
    if kind in ("hive",) and not cfg.get("host"):
        raise C.QueryError("Host is required.")
    if kind == "hue":
        if cfg.get("url") and not re.match(r"^https?://", cfg["url"], re.I):
            cfg["url"] = "http://" + cfg["url"]
        if not re.match(r"^https?://[A-Za-z0-9.\-]+(:\d{1,5})?(/[^\s]*)?$", cfg.get("url", "")):
            raise C.QueryError("Hue address: enter the full address, e.g. http://huehost.company.com:8888")
    if kind == "redshift":
        if cfg.get("auth_mode", "password") == "password" and not cfg.get("host"):
            raise C.QueryError("Endpoint host is required for password sign-in.")
        if cfg.get("auth_mode") == "iam" and not (cfg.get("cluster_identifier") or cfg.get("serverless_workgroup") or cfg.get("host")):
            raise C.QueryError("Enter the cluster identifier or serverless workgroup for IAM sign-in.")
    if kind == "sqlite":
        base = os.path.realpath(DATA_DIR)
        if not os.path.realpath(os.path.join(base, cfg["path"])).startswith(base + os.sep):
            raise C.QueryError("Demo database files must be inside the data folder.")
    for k in cfg:
        if re.search(r"(passw|secret_access|token)", k):
            raise C.QueryError("Passwords and keys can't be stored in connection settings.")
    return dict(name=name, type=kind, description=str(body.get("description", "")).strip()[:200],
                config_json=json.dumps(cfg), secret_mode=mode, secret_ref=ref if mode in ("env", "aws_secret") else "")


@app.post("/api/admin/connections")
@admin_required
def admin_create_connection():
    try:
        v = _validate_conn_body(request.get_json(force=True, silent=True) or {})
        db().execute("INSERT INTO connections(name, type, description, config_json, secret_mode, secret_ref, created_at)"
                     " VALUES (?,?,?,?,?,?,?)", (v["name"], v["type"], v["description"], v["config_json"],
                                                 v["secret_mode"], v["secret_ref"], now()))
        db().commit()
    except C.QueryError as exc:
        return jsonify(error=str(exc)), 400
    except sqlite3.IntegrityError:
        return jsonify(error="A connection with that name already exists."), 400
    log("connection_created", v["name"])
    return jsonify(ok=True), 201


@app.patch("/api/admin/connections/<int:cid>")
@admin_required
def admin_update_connection(cid):
    body = request.get_json(force=True, silent=True) or {}
    row = db().execute("SELECT * FROM connections WHERE id=?", (cid,)).fetchone()
    if not row:
        abort(404)
    if set(body) == {"active"}:
        db().execute("UPDATE connections SET active=?, updated_at=? WHERE id=?", (1 if body["active"] else 0, now(), cid))
        db().commit()
        log("connection_" + ("enabled" if body["active"] else "disabled"), row["name"])
        return jsonify(ok=True)
    try:
        v = _validate_conn_body(body)
        db().execute("UPDATE connections SET name=?, type=?, description=?, config_json=?, secret_mode=?, secret_ref=?,"
                     " updated_at=? WHERE id=?", (v["name"], v["type"], v["description"], v["config_json"],
                                                  v["secret_mode"], v["secret_ref"], now(), cid))
        db().commit()
    except C.QueryError as exc:
        return jsonify(error=str(exc)), 400
    except sqlite3.IntegrityError:
        return jsonify(error="A connection with that name already exists."), 400
    log("connection_updated", v["name"])
    return jsonify(ok=True)


@app.delete("/api/admin/connections/<int:cid>")
@admin_required
def admin_delete_connection(cid):
    row = db().execute("SELECT * FROM connections WHERE id=?", (cid,)).fetchone()
    if not row:
        abort(404)
    db().execute("DELETE FROM connections WHERE id=?", (cid,))
    db().commit()
    log("connection_deleted", row["name"])
    return jsonify(ok=True)


@app.get("/api/admin/activity")
@admin_required
def admin_activity():
    rows = db().execute("SELECT * FROM activity ORDER BY id DESC LIMIT 300").fetchall()
    return jsonify([dict(r) for r in rows])


@app.errorhandler(400)
@app.errorhandler(404)
@app.errorhandler(413)
def api_error(e):
    msg = getattr(e, "description", "Request failed.")
    if e.code == 413:
        msg = "The report is too large to save."
    if request.path.startswith("/api/"):
        return jsonify(error=msg), e.code
    return msg, e.code


init_db()
purge_old_runs()
if os.environ.get("RECON_PURGE_THREAD", "1") == "1":
    threading.Thread(target=_purge_loop, name="recon-cleanup", daemon=True).start()


def _cli_reset_password(username):
    """python app.py reset-password <username>  -> prints a new temporary password."""
    temp = secrets.token_urlsafe(9)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("UPDATE users SET password_hash=?, must_change=1, active=1 WHERE username=?",
                       (generate_password_hash(temp), username))
    conn.commit()
    conn.close()
    if cur.rowcount:
        print(f"Temporary password for {username}: {temp}  (they must change it at sign-in)")
    else:
        print(f"No user called {username}.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2 and sys.argv[1] == "purge":
        print(f"Removed {purge_old_runs()} run(s) older than {RETENTION_DAYS} days.")
        raise SystemExit(0)
    if len(sys.argv) == 3 and sys.argv[1] == "reset-password":
        _cli_reset_password(sys.argv[2])
        raise SystemExit(0)
    app.run(load_dotenv=False, host=os.environ.get("RECON_HOST", "127.0.0.1"),
            port=int(os.environ.get("RECON_PORT", "5000")), debug=False)
