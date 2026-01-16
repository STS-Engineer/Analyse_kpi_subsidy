# app.py
import os
import re
import json
import smtplib
import atexit
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Any, Set, Tuple, Callable

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # type: ignore

import requests
import psycopg2
from psycopg2 import sql
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

ALWAYS_CC_EMAIL = "administration.STS@avocarbon.com"

# =============================================================================
# Env helpers (no .env, everything has defaults)
# =============================================================================
def _first_env(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _get_timezone(tz_name: str):
    tz_name = (tz_name or "").strip()
    if not tz_name:
        return None
    if ZoneInfo is not None:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            pass
    try:
        import pytz  # type: ignore
        return pytz.timezone(tz_name)
    except Exception:
        return None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# Flask & CORS
# =============================================================================
app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIG
# =============================================================================

# PostgreSQL
DB_CONFIG = {
    "host": _first_env(["PGHOST", "DB_HOST"], "avo-adb-002.postgres.database.azure.com"),
    "database": _first_env(["PGDATABASE", "DB_NAME"], "Subsidy_DB"),
    "user": _first_env(["PGUSER", "DB_USER"], "administrationSTS"),
    "password": _first_env(["PGPASSWORD", "DB_PASSWORD"], "St$@0987"),
    "port": int(_first_env(["PGPORT", "DB_PORT"], "5432")),
    "sslmode": _first_env(["PGSSLMODE"], "require"),
}

# Email settings
SMTP_SERVER = _first_env(["SMTP_SERVER"], "avocarbon-com.mail.protection.outlook.com")
SMTP_PORT = int(_first_env(["SMTP_PORT"], "25"))
EMAIL_USER = _first_env(["EMAIL_USER"], "administration.STS@avocarbon.com")
EMAIL_PASSWORD = _first_env(["EMAIL_PASSWORD"], "")  # relay mode usually empty

SMTP_FALLBACK_SERVER = _first_env(["SMTP_FALLBACK_SERVER"], SMTP_SERVER)
SMTP_FALLBACK_PORT = int(_first_env(["SMTP_FALLBACK_PORT"], "25"))
SMTP_AUTH_MODE = _first_env(["SMTP_AUTH_MODE"], "auto").lower()  # auto | login | none
SMTP_ALLOW_NO_AUTH_FALLBACK = _env_bool("SMTP_ALLOW_NO_AUTH_FALLBACK", True)

SMTP_INTERNAL_DOMAINS = [
    d.strip().lower()
    for d in _first_env(["SMTP_INTERNAL_DOMAINS"], "avocarbon.com").split(",")
    if d.strip()
]

# monday.com
MONDAY_API_KEY = _first_env(["MONDAY_API_KEY"], "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjUzNzU5MzQ0NywiYWFpIjoxMSwidWlkIjo3NjQ5MDYwMiwiaWFkIjoiMjAyNS0wNy0xMFQxNTo0Mzo0OS4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NDUyNTc0NywicmduIjoidXNlMSJ9.MhRXxTDVZlx2FSnPii_PZ8dD39Q_kCdZXsrEjOCt4i4")
MONDAY_BOARD_REQUESTS_ID = int(_first_env(["MONDAY_BOARD_REQUESTS_ID"], "9612741617"))
MONDAY_BOARD_ACTIONS_ID = int(_first_env(["MONDAY_BOARD_ACTIONS_ID"], "9366723818"))
MONDAY_BASE_URL = _first_env(["MONDAY_BASE_URL"], "https://avocarbon.monday.com")

# Actions board column IDs
MONDAY_ACTION_ITEM_ID_COL_ID = _first_env(["MONDAY_ACTION_ITEM_ID_COL_ID"], "pulse_id_mkzk18n7")
MONDAY_ACTION_DETAIL_COL_ID = _first_env(["MONDAY_ACTION_DETAIL_COL_ID"], "").strip()
MONDAY_ACTION_TYPE_COL_ID = "text_mkrva3a0"  # Type of Action -> action_type

# Requests board column IDs
REQUEST_DATE_CREATION_COL_ID = "date_mkvttsk8"
REQUEST_SITE_LOCATION_COL_ID = "dropdown_mksyv6w3"
REQUEST_TYPE_COL_ID = "color_mkvt418p"
REQUEST_DECISION_COL_ID = "color_mkv7m2s3"
REQUEST_VALIDATED_COL_ID = "long_text_mkvc4t53"
REQUEST_REJECTED_COL_ID = "long_text_mkvckt4k"
REQUEST_ADDITIONAL_NOTES_COL_ID = "text_mkv770c5"
REQUEST_SUBSIDY_OUTCOME_COL_ID = "color_mkw23bbc"
REQUEST_AWARDED_PROGRAMS_COL_ID = "text_mkw89xw4"
REQUEST_AMOUNT_AWARDED_COL_ID = "numeric_mkw2vnds"
REQUEST_DATE_RECEIPT_COL_ID = "date_mkw383ge"
REQUEST_ESTIMATED_BUDGET_NUM_COL_ID = "numeric_mkwrdg3z"
REQUEST_APPLICANT_NAME_COL_ID = "multiple_person_mkt3wpzn"

# DB tables / keys
DB_TABLE_REQUESTS = _first_env(["DB_TABLE_REQUESTS"], "subsidy_requests")
DB_TABLE_ACTIONS = _first_env(["DB_TABLE_ACTIONS"], "subsidy_action_plan")
DB_KEY_REQUESTS = _first_env(["DB_KEY_REQUESTS"], "element_id")
DB_KEY_ACTIONS = _first_env(["DB_KEY_ACTIONS"], "action_id")

# ALWAYS filter Actions board to only items where Assistant Generator == "AI Subsidy Assistant"
MONDAY_ACTIONS_ASSISTANT_COL_ID = _first_env(["MONDAY_ACTIONS_ASSISTANT_COL_ID"], "text_mks2y5v7").strip()
MONDAY_ACTIONS_ASSISTANT_VALUE = "AI Subsidy Assistant"

# Owner mapping
MONDAY_ACTION_OWNER_COL_ID = _first_env(["MONDAY_ACTION_OWNER_COL_ID"], "multiple_person_mkv090pp").strip()
DB_ACTION_OWNER_COL = _first_env(["DB_ACTION_OWNER_COL"], "action_owner").strip()

# Reminder schedule
REMINDER_DAY_OF_WEEK = _first_env(["REMINDER_DAY_OF_WEEK"], "fri").strip().lower()
REMINDER_HOUR = _env_int("REMINDER_HOUR", 10)
REMINDER_MINUTE = _env_int("REMINDER_MINUTE", 50)
REMINDER_TIMEZONE = _first_env(["REMINDER_TIMEZONE"], "Africa/Tunis").strip()
REMINDER_TZINFO = _get_timezone(REMINDER_TIMEZONE)

# Background jobs toggles
ENABLE_BACKGROUND_JOBS = _env_bool("ENABLE_BACKGROUND_JOBS", True)
ENABLE_POLLING_SYNC = _env_bool("ENABLE_POLLING_SYNC", True)  # we will schedule sync via CronTrigger
ENABLE_EMAIL_SCHEDULER = _env_bool("ENABLE_EMAIL_SCHEDULER", True)

# Optional: DB advisory lock to avoid duplicates if multiple workers/instances
ENABLE_DB_ADVISORY_LOCK = _env_bool("ENABLE_DB_ADVISORY_LOCK", True)
LOCK_KEY_SYNC = _env_int("LOCK_KEY_SYNC", 10001)
LOCK_KEY_REMINDERS = _env_int("LOCK_KEY_REMINDERS", 20001)

# =============================================================================
# Explicit mapping: monday column_id -> DB column name
# =============================================================================
REQUESTS_COLID_TO_DB: Dict[str, str] = {
    REQUEST_DATE_CREATION_COL_ID: "date_creation",
    REQUEST_SITE_LOCATION_COL_ID: "site_location",
    REQUEST_TYPE_COL_ID: "request_type",
    REQUEST_DECISION_COL_ID: "decision",
    REQUEST_VALIDATED_COL_ID: "validated_subventions",
    REQUEST_REJECTED_COL_ID: "rejected_subventions",
    REQUEST_ADDITIONAL_NOTES_COL_ID: "additional_notes",
    REQUEST_SUBSIDY_OUTCOME_COL_ID: "subsidy_outcome",
    REQUEST_AWARDED_PROGRAMS_COL_ID: "Names of Awarded Subvention Programs",
    REQUEST_AMOUNT_AWARDED_COL_ID: "amount_awarded",
    REQUEST_DATE_RECEIPT_COL_ID: "date_of_subsidy_receipt",
    REQUEST_ESTIMATED_BUDGET_NUM_COL_ID: "estimated_budget",
    REQUEST_APPLICANT_NAME_COL_ID: "applicant_name",
}

ACTIONS_COLID_TO_DB: Dict[str, str] = {
    MONDAY_ACTION_TYPE_COL_ID: "action_type",
    MONDAY_ACTIONS_ASSISTANT_COL_ID: "assistant_generator",
}

# =============================================================================
# Helpers
# =============================================================================
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        raise Exception(f"Database connection error: {e}")


def sanitize_column_name(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "_", t)
    return t.strip("_")


def normalize_value(text: Any) -> Any:
    if text is None:
        return None
    if isinstance(text, str):
        s = text.strip()
        if s == "":
            return None
        if re.fullmatch(r"-?\d+", s):
            try:
                return int(s)
            except Exception:
                return s
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            try:
                return float(s)
            except Exception:
                return s
        return s
    return text


def get_table_columns(conn, table: str) -> Set[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        return {r[0] for r in cur.fetchall()}


# =============================================================================
# Advisory lock helpers (prevents duplicates with multi-workers)
# =============================================================================
def _try_advisory_lock(conn, lock_key: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
        row = cur.fetchone()
        return bool(row and row[0])


def _unlock_advisory_lock(conn, lock_key: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))


def run_with_db_lock(lock_key: int, fn: Callable[[], Any], label: str) -> Any:
    """
    Holds an advisory lock for the duration of fn().
    Uses a dedicated DB connection to hold the lock.
    """
    if not ENABLE_DB_ADVISORY_LOCK:
        return fn()

    conn = get_db_connection()
    try:
        if not _try_advisory_lock(conn, lock_key):
            print(f"[lock:{label}] Skipped (lock already held)", flush=True)
            return {"skipped": True, "reason": "lock_held", "label": label, "ts": utc_now().isoformat()}
        try:
            return fn()
        finally:
            try:
                _unlock_advisory_lock(conn, lock_key)
            except Exception:
                pass
    finally:
        conn.close()


# =============================================================================
# monday.com API
# =============================================================================
def monday_query(api_key: str, query: str) -> Dict[str, Any]:
    api_url = "https://api.monday.com/v2"
    headers = {"Authorization": api_key}
    resp = requests.post(api_url, json={"query": query}, headers=headers, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"monday API error {resp.status_code}: {resp.text}")
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"monday API returned errors: {data['errors']}")
    return data


def query_monday_items(api_key: str, board_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    query = f"""
    {{
      boards(ids: {board_id}) {{
        items_page(limit: {limit}) {{
          items {{
            id
            name
            column_values {{
              id
              text
              value
              column {{ title }}
            }}
          }}
        }}
      }}
    }}
    """
    data = monday_query(api_key, query)
    boards = data.get("data", {}).get("boards", [])
    if not boards:
        return []
    return boards[0].get("items_page", {}).get("items", [])


def query_monday_actions_filtered(
    api_key: str,
    board_id: int,
    column_id: str,
    column_value: str,
    limit: int = 100
) -> List[Dict[str, Any]]:
    query = f"""
    {{
      items_page_by_column_values(
        board_id: {board_id},
        columns: [{{ column_id: "{column_id}", column_values: ["{column_value}"] }}],
        limit: {limit}
      ) {{
        items {{
          id
          name
          column_values {{
            id
            text
            value
            column {{ title }}
          }}
        }}
      }}
    }}
    """
    data = monday_query(api_key, query)
    return data.get("data", {}).get("items_page_by_column_values", {}).get("items", [])


# =============================================================================
# People column -> emails
# =============================================================================
def extract_people_ids_from_value(value_str: Optional[str]) -> List[int]:
    if not value_str:
        return []
    try:
        data = json.loads(value_str)
    except Exception:
        return []
    pts = data.get("personsAndTeams", []) or []
    ids: List[int] = []
    for x in pts:
        if x.get("kind") == "person" and "id" in x:
            try:
                ids.append(int(x["id"]))
            except Exception:
                pass
    return ids


def fetch_user_emails(api_key: str, user_ids: List[int]) -> Dict[int, str]:
    if not user_ids:
        return {}
    unique_ids = sorted(set(int(i) for i in user_ids))
    ids_csv = ",".join(str(i) for i in unique_ids)
    query = f"""
    {{
      users(ids: [{ids_csv}]) {{
        id
        email
      }}
    }}
    """
    data = monday_query(api_key, query)
    users = data.get("data", {}).get("users", []) or []
    out: Dict[int, str] = {}
    for u in users:
        try:
            uid = int(u["id"])
            out[uid] = (u.get("email") or "").strip()
        except Exception:
            pass
    return out


# =============================================================================
# Mapping monday -> DB row
# =============================================================================
def map_item_to_db_values(
    item: Dict[str, Any],
    db_columns: Set[str],
    key_col: str,
    monday_item_id: int,
    *,
    colid_to_db: Optional[Dict[str, str]] = None,
    is_actions_table: bool = False,
) -> Dict[str, Any]:
    values: Dict[str, Any] = {}

    if key_col in db_columns:
        values[key_col] = monday_item_id

    if "name" in db_columns:
        values["name"] = item.get("name", "")

    if "updated_at" in db_columns:
        values["updated_at"] = utc_now()

    for cv in item.get("column_values", []) or []:
        col_id = cv.get("id")
        text_val = normalize_value(cv.get("text"))

        # 1) explicit mapping
        if colid_to_db and col_id in colid_to_db:
            db_col = colid_to_db[col_id]
            if db_col in db_columns and text_val is not None:
                values[db_col] = text_val
            continue

        # 2) fallback mapping by title
        col = cv.get("column") or {}
        title = col.get("title") or ""
        db_guess = sanitize_column_name(title)
        if db_guess in db_columns and text_val is not None:
            values[db_guess] = text_val

    # Special: action_detail
    if is_actions_table and "action_detail" in db_columns:
        if MONDAY_ACTION_DETAIL_COL_ID:
            for cv in item.get("column_values", []) or []:
                if cv.get("id") == MONDAY_ACTION_DETAIL_COL_ID:
                    v = normalize_value(cv.get("text"))
                    if v is not None:
                        values["action_detail"] = v
                    break
        else:
            for cv in item.get("column_values", []) or []:
                col = cv.get("column") or {}
                title = (col.get("title") or "").strip().lower()
                if title in {
                    "ai subsidy assistant",
                    "action détaillé",
                    "action detaille",
                    "action detail",
                    "detailed action",
                    "action details",
                }:
                    v = normalize_value(cv.get("text"))
                    if v is not None:
                        values["action_detail"] = v
                    break

    # Special: item_link
    if is_actions_table and "item_link" in db_columns:
        item_id_str: str = ""
        for cv in item.get("column_values", []) or []:
            if cv.get("id") == MONDAY_ACTION_ITEM_ID_COL_ID:
                item_id_str = (cv.get("text") or "").strip()
                break
        if item_id_str:
            values["item_link"] = f"{MONDAY_BASE_URL}/boards/{MONDAY_BOARD_ACTIONS_ID}/pulses/{item_id_str}"

    return values


# =============================================================================
# DB upsert/delete
# =============================================================================
def exists_by_key(cur, table: str, key_col: str, key_val: int) -> bool:
    q = sql.SQL("SELECT 1 FROM {} WHERE {} = %s").format(
        sql.Identifier(table),
        sql.Identifier(key_col),
    )
    cur.execute(q, (key_val,))
    return cur.fetchone() is not None


def insert_row(cur, table: str, values: Dict[str, Any]) -> None:
    cols = list(values.keys())
    q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in cols),
    )
    cur.execute(q, [values[c] for c in cols])


def update_row(cur, table: str, key_col: str, key_val: int, values: Dict[str, Any]) -> None:
    upd_cols = [c for c in values.keys() if c != key_col]
    if not upd_cols:
        return
    q = sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in upd_cols),
        sql.Identifier(key_col),
    )
    params = [values[c] for c in upd_cols] + [key_val]
    cur.execute(q, params)


def upsert_by_key(cur, table: str, key_col: str, values: Dict[str, Any], *, db_columns: Set[str]) -> str:
    if key_col not in values:
        raise RuntimeError(f"Missing key '{key_col}' for table {table}")

    key_val = int(values[key_col])

    if not exists_by_key(cur, table, key_col, key_val):
        if "created_at" in db_columns and "created_at" not in values:
            values["created_at"] = utc_now()
        insert_row(cur, table, values)
        return "insert"

    update_row(cur, table, key_col, key_val, values)
    return "update"


def delete_missing_by_key(cur, table: str, key_col: str, keep_ids: Set[int]) -> int:
    if not keep_ids:
        q = sql.SQL("DELETE FROM {} WHERE {} IS NOT NULL").format(
            sql.Identifier(table),
            sql.Identifier(key_col),
        )
        cur.execute(q)
        return cur.rowcount

    keep_list = list(keep_ids)
    deleted = 0
    chunk = 1000

    for i in range(0, len(keep_list), chunk):
        part = keep_list[i:i + chunk]
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in part)
        q = sql.SQL("DELETE FROM {} WHERE {} IS NOT NULL AND {} NOT IN ({})").format(
            sql.Identifier(table),
            sql.Identifier(key_col),
            sql.Identifier(key_col),
            placeholders,
        )
        cur.execute(q, part)
        deleted += cur.rowcount

    return deleted


# =============================================================================
# FULL SYNC
# =============================================================================
def fetch_monday_items_for_sync() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (requests_items, actions_items)
    Actions are ALWAYS filtered by Assistant Generator == "AI Subsidy Assistant".
    """
    if not MONDAY_API_KEY:
        raise RuntimeError("MONDAY_API_KEY is missing")

    if not MONDAY_ACTIONS_ASSISTANT_COL_ID:
        raise RuntimeError(
            "MONDAY_ACTIONS_ASSISTANT_COL_ID is required. "
            "Set it to the monday column ID for 'Assistant Generator'."
        )

    requests_items = query_monday_items(MONDAY_API_KEY, MONDAY_BOARD_REQUESTS_ID, limit=100)

    actions_items = query_monday_actions_filtered(
        MONDAY_API_KEY,
        MONDAY_BOARD_ACTIONS_ID,
        MONDAY_ACTIONS_ASSISTANT_COL_ID,
        MONDAY_ACTIONS_ASSISTANT_VALUE,
        limit=100,
    )

    return requests_items, actions_items


def perform_full_sync() -> Dict[str, Any]:
    requests_items, actions_items = fetch_monday_items_for_sync()

    summary = {
        "requests": {"inserted": 0, "updated": 0, "deleted": 0, "count_monday": len(requests_items)},
        "actions": {"inserted": 0, "updated": 0, "deleted": 0, "count_monday": len(actions_items)},
        "timestamp": utc_now().isoformat(),
    }

    conn = get_db_connection()
    try:
        req_cols = get_table_columns(conn, DB_TABLE_REQUESTS)
        act_cols = get_table_columns(conn, DB_TABLE_ACTIONS)

        # Pre-fetch ALL user emails used in Actions
        all_people_ids: List[int] = []
        for it in actions_items:
            for cv in it.get("column_values", []) or []:
                if cv.get("id") == MONDAY_ACTION_OWNER_COL_ID:
                    all_people_ids += extract_people_ids_from_value(cv.get("value"))
        emails_map = fetch_user_emails(MONDAY_API_KEY, all_people_ids)

        with conn:
            with conn.cursor() as cur:
                # -------- Requests
                request_ids: Set[int] = set()
                for it in requests_items:
                    mid = int(it["id"])
                    request_ids.add(mid)

                    vals = map_item_to_db_values(
                        it,
                        req_cols,
                        DB_KEY_REQUESTS,
                        mid,
                        colid_to_db=REQUESTS_COLID_TO_DB,
                        is_actions_table=False,
                    )

                    action = upsert_by_key(cur, DB_TABLE_REQUESTS, DB_KEY_REQUESTS, vals, db_columns=req_cols)
                    if action == "insert":
                        summary["requests"]["inserted"] += 1
                    else:
                        summary["requests"]["updated"] += 1

                summary["requests"]["deleted"] = delete_missing_by_key(
                    cur, DB_TABLE_REQUESTS, DB_KEY_REQUESTS, request_ids
                )

                # -------- Actions
                action_ids: Set[int] = set()
                for it in actions_items:
                    mid = int(it["id"])
                    action_ids.add(mid)

                    vals = map_item_to_db_values(
                        it,
                        act_cols,
                        DB_KEY_ACTIONS,
                        mid,
                        colid_to_db=ACTIONS_COLID_TO_DB,
                        is_actions_table=True,
                    )

                    # action_owner email from People column
                    owner_email: Optional[str] = None
                    for cv in it.get("column_values", []) or []:
                        if cv.get("id") == MONDAY_ACTION_OWNER_COL_ID:
                            person_ids = extract_people_ids_from_value(cv.get("value"))
                            for pid in person_ids:
                                em = (emails_map.get(pid) or "").strip()
                                if em:
                                    owner_email = em
                                    break
                        if owner_email:
                            break

                    if DB_ACTION_OWNER_COL in act_cols and owner_email:
                        vals[DB_ACTION_OWNER_COL] = owner_email

                    action = upsert_by_key(cur, DB_TABLE_ACTIONS, DB_KEY_ACTIONS, vals, db_columns=act_cols)
                    if action == "insert":
                        summary["actions"]["inserted"] += 1
                    else:
                        summary["actions"]["updated"] += 1

                summary["actions"]["deleted"] = delete_missing_by_key(
                    cur, DB_TABLE_ACTIONS, DB_KEY_ACTIONS, action_ids
                )

    finally:
        conn.close()

    return summary


# =============================================================================
# EMAIL FUNCTIONS
# =============================================================================
def send_email(to_email: str, subject: str, body_html: str) -> bool:
    """
    Send an email using the configured SMTP settings.
    Always adds administrationSTS in CC on every email.
    """
    def _email_domain(addr: str) -> str:
        addr = (addr or "").strip().lower()
        if "@" not in addr:
            return ""
        return addr.split("@", 1)[1]

    def _is_internal_recipient(addr: str) -> bool:
        if not SMTP_INTERNAL_DOMAINS:
            return True
        return _email_domain(addr) in set(SMTP_INTERNAL_DOMAINS)

    server_host = (SMTP_SERVER or "").strip()
    server_port = int(SMTP_PORT)

    if not server_host:
        print("[email] SMTP_SERVER is empty.", flush=True)
        return False

    auth_mode = (SMTP_AUTH_MODE or "auto").strip().lower()
    looks_like_o365_mx = "mail.protection.outlook.com" in server_host.lower()

    if auth_mode in {"none", "no", "false", "0"}:
        use_auth = False
    elif auth_mode in {"login", "auth", "true", "1"}:
        use_auth = True
    else:
        use_auth = bool(EMAIL_PASSWORD)
        if server_port == 25 and looks_like_o365_mx:
            use_auth = False

    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_USER or ""
    msg["To"] = to_email
    msg["Cc"] = ALWAYS_CC_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body_html, "html"))

    recipients = [to_email]
    if ALWAYS_CC_EMAIL:
        recipients.append(ALWAYS_CC_EMAIL)

    def _smtp_send(host: str, port: int, *, do_auth: bool) -> None:
        if not host:
            raise RuntimeError("SMTP host is empty")

        if not do_auth:
            for r in recipients:
                if not _is_internal_recipient(r):
                    raise RuntimeError(
                        f"Refusing unauthenticated send to external domain: {r} (allowed: {SMTP_INTERNAL_DOMAINS})"
                    )

        with smtplib.SMTP(host, port, timeout=30) as server:
            server.ehlo()
            want_starttls = (port == 587) or _env_bool("SMTP_USE_STARTTLS", True)
            if want_starttls and server.has_extn("starttls"):
                server.starttls()
                server.ehlo()

            if do_auth:
                if not EMAIL_USER or not EMAIL_PASSWORD:
                    raise RuntimeError("EMAIL_USER / EMAIL_PASSWORD not set for SMTP AUTH")
                server.login(EMAIL_USER, EMAIL_PASSWORD)

            server.sendmail(msg["From"], recipients, msg.as_string())

    try:
        _smtp_send(server_host, server_port, do_auth=use_auth)
        print(
            f"[email] Sent to {to_email} (cc={ALWAYS_CC_EMAIL}) via {server_host}:{server_port} (auth={use_auth})",
            flush=True
        )
        return True

    except smtplib.SMTPAuthenticationError as e:
        err = str(e)
        print(f"[email] SMTP AUTH failed via {server_host}:{server_port}: {err}", flush=True)

        basic_auth_disabled = ("5.7.139" in err) or ("basic authentication is disabled" in err.lower())

        if SMTP_ALLOW_NO_AUTH_FALLBACK and basic_auth_disabled:
            fb_host = (SMTP_FALLBACK_SERVER or "").strip()
            fb_port = int(SMTP_FALLBACK_PORT)
            if fb_host and (fb_host != server_host or fb_port != server_port):
                try:
                    _smtp_send(fb_host, fb_port, do_auth=False)
                    print(
                        f"[email] Sent to {to_email} (cc={ALWAYS_CC_EMAIL}) via fallback {fb_host}:{fb_port} (auth=False)",
                        flush=True
                    )
                    return True
                except Exception as e2:
                    print(f"[email] Fallback send failed via {fb_host}:{fb_port}: {e2}", flush=True)

        return False

    except Exception as e:
        print(f"[email] Failed to send to {to_email} via {server_host}:{server_port}: {e}", flush=True)
        return False


def get_late_actions() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        query = sql.SQL("""
        SELECT
            id,
            name,
            action_id,
            item_link,
            action_detail,
            action_owner,
            project_title,
            status,
            action_type,
            due_date,
            plant
        FROM public.{table}
        WHERE status = 'Late' AND action_owner IS NOT NULL AND action_owner != ''
        """).format(table=sql.Identifier(DB_TABLE_ACTIONS))

        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def create_reminder_email_body(owner_email: str, actions: List[Dict[str, Any]]) -> str:
    def _action_li(a: Dict[str, Any]) -> str:
        name = (a.get("name") or "Untitled action").strip()
        detail = (a.get("action_detail") or "").strip()
        link = (a.get("item_link") or "").strip()
        name_html = f'<a href="{link}" target="_blank" rel="noopener noreferrer">{name}</a>' if link else name
        suffix = f" – {detail}" if detail else ""
        return f"<li>{name_html}{suffix}</li>"

    items_html = "\n".join(_action_li(a) for a in actions)

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 760px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #f3f4f6; border: 1px solid #e5e7eb; padding: 16px 20px; }}
            .content {{ background-color: #ffffff; border: 1px solid #e5e7eb; border-top: none; padding: 20px; }}
            ul {{ margin-top: 8px; }}
            li {{ margin: 8px 0; }}
            .hint {{ color: #6b7280; font-size: 12px; margin-top: 18px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div style="font-size:16px; font-weight:bold;">Hello,</div>
            </div>
            <div class="content">
                <p>Below is the list of <b>late subsidy actions</b> that require an update or follow-up from your side:</p>
                <ul>{items_html}</ul>
                <p>Please share a progress update or close the actions that are already completed.</p>
                <p>Best regards,<br/>AI Subsidy Assistant</p>
                <div class="hint">This is an automated reminder sent to {owner_email}. Please do not reply to this email.</div>
            </div>
        </div>
    </body>
    </html>
    """


def send_late_action_reminders() -> Dict[str, Any]:
    print("[cron] Starting weekly late action check...", flush=True)
    try:
        late_actions = get_late_actions()
        if not late_actions:
            return {"success": True, "late_actions_count": 0, "emails_sent": 0, "emails_failed": 0, "message": "No late actions found"}

        actions_by_owner: Dict[str, List[Dict[str, Any]]] = {}
        for action in late_actions:
            owner_email = (action.get("action_owner") or "").strip()
            if owner_email:
                actions_by_owner.setdefault(owner_email, []).append(action)

        emails_sent = 0
        emails_failed = 0

        for owner_email, owner_actions in actions_by_owner.items():
            subject = f"Follow-up required: {len(owner_actions)} late subsidy action(s)"
            body = create_reminder_email_body(owner_email, owner_actions)
            if send_email(owner_email, subject, body):
                emails_sent += 1
            else:
                emails_failed += 1

        return {
            "success": True,
            "late_actions_count": len(late_actions),
            "owners_count": len(actions_by_owner),
            "emails_sent": emails_sent,
            "emails_failed": emails_failed,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        return {"success": False, "error": str(e), "timestamp": datetime.now().isoformat()}


# =============================================================================
# Scheduled wrappers (lock + logs)
# =============================================================================
def _scheduled_sync_job():
    try:
        print("[auto_sync] Starting sync job...", flush=True)
        result = run_with_db_lock(LOCK_KEY_SYNC, perform_full_sync, "sync")
        print(f"[auto_sync] Finished: {result}", flush=True)
        return result
    except Exception as e:
        print(f"[auto_sync] ERROR: {e}", flush=True)
        raise


def _scheduled_reminders_job():
    try:
        print("[reminders] Starting reminders job...", flush=True)
        result = run_with_db_lock(LOCK_KEY_REMINDERS, send_late_action_reminders, "reminders")
        print(f"[reminders] Finished: {result}", flush=True)
        return result
    except Exception as e:
        print(f"[reminders] ERROR: {e}", flush=True)
        raise


# =============================================================================
# KPI data / calculations (unchanged)
# =============================================================================
def get_subsidy_requests_data(connection) -> pd.DataFrame:
    query = sql.SQL("""
    SELECT
        id,
        name,
        applicant_name,
        site_location,
        decision,
        date_creation,
        request_type,
        estimated_budget,
        subsidy_outcome,
        amount_awarded,
        date_of_subsidy_receipt
    FROM public.{table}
    """).format(table=sql.Identifier(DB_TABLE_REQUESTS))
    return pd.read_sql_query(query, connection)


def get_action_plan_data(connection) -> pd.DataFrame:
    query = sql.SQL("""
    SELECT
        id,
        name,
        action_id,
        action_owner,
        project_title,
        status,
        action_type,
        initiation_date,
        due_date,
        plant,
        expected_gain
    FROM public.{table}
    """).format(table=sql.Identifier(DB_TABLE_ACTIONS))
    return pd.read_sql_query(query, connection)


def _clean_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def calculate_global_kpis(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict:
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    df["estimated_budget"] = pd.to_numeric(df.get("estimated_budget"), errors="coerce").fillna(0)
    df["amount_awarded"] = pd.to_numeric(df.get("amount_awarded"), errors="coerce").fillna(0)

    for col in ["request_type", "decision", "subsidy_outcome", "site_location"]:
        if col in df.columns:
            df[col] = _clean_text(df[col])

    total_requests = len(df)
    spontaneous_mask = df["request_type"].eq("Spontaneous requests") if "request_type" in df.columns else pd.Series([], dtype=bool)
    planned_mask = df["request_type"].eq("Planned requests") if "request_type" in df.columns else pd.Series([], dtype=bool)
    validated_mask = df["decision"].isin(["Validate all subventions", "Validate some subventions"]) if "decision" in df.columns else pd.Series([], dtype=bool)
    any_answer_mask = df["decision"].notna() if "decision" in df.columns else pd.Series([], dtype=bool)
    won_mask = df["subsidy_outcome"].eq("Won") if "subsidy_outcome" in df.columns else pd.Series([], dtype=bool)
    in_progress_mask = df["decision"].isna() if "decision" in df.columns else pd.Series([], dtype=bool)

    kpis = {}
    kpis["nombre_demandes_spontanees"] = int(spontaneous_mask.sum())
    kpis["montants_en_cours"] = float(df.loc[in_progress_mask, "estimated_budget"].sum()) if "estimated_budget" in df.columns else 0.0
    kpis["nombre_demandes_validees"] = int(validated_mask.sum())
    kpis["montants_obtenus"] = float(df["amount_awarded"].sum()) if "amount_awarded" in df.columns else 0.0

    kpis["actions_realisees"] = int((ap["status"] == "Completed").sum()) if "status" in ap.columns else 0
    kpis["actions_en_retard"] = int((ap["status"] == "Late").sum()) if "status" in ap.columns else 0

    kpis["plant_requests"] = int(planned_mask.sum())
    denom_validated = max(int(validated_mask.sum()), 1)
    kpis["success_rate_validated"] = float((won_mask & validated_mask).sum() / denom_validated * 100)
    kpis["success_rate_submitted_alt"] = float(validated_mask.sum() / max(total_requests, 1) * 100)

    total_est = float(df["estimated_budget"].sum()) if "estimated_budget" in df.columns else 0.0
    total_awd = float(df["amount_awarded"].sum()) if "amount_awarded" in df.columns else 0.0
    kpis["impact_competitiveness"] = float((total_awd / total_est * 100) if total_est > 0 else 0.0)

    kpis["percent_positive_answers"] = float(validated_mask.sum() / max(total_requests, 1) * 100)
    kpis["percent_any_answer_alt"] = float(any_answer_mask.sum() / max(total_requests, 1) * 100)

    return kpis


def calculate_kpis_by_site(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    df["estimated_budget"] = pd.to_numeric(df.get("estimated_budget"), errors="coerce").fillna(0)
    df["amount_awarded"] = pd.to_numeric(df.get("amount_awarded"), errors="coerce").fillna(0)

    for col in ["request_type", "decision", "subsidy_outcome", "site_location"]:
        if col in df.columns:
            df[col] = _clean_text(df[col])
    if "plant" in ap.columns:
        ap["plant"] = _clean_text(ap["plant"])

    sites = sorted(set(df.get("site_location", pd.Series([])).dropna().unique()) | set(ap.get("plant", pd.Series([])).dropna().unique()))
    rows: List[Dict] = []

    for site in sites:
        site_req = df[df["site_location"] == site] if "site_location" in df.columns else df.iloc[0:0]
        site_ap = ap[ap["plant"] == site] if "plant" in ap.columns else ap.iloc[0:0]

        total = len(site_req)
        spontaneous_mask = site_req["request_type"].eq("Spontaneous requests") if "request_type" in site_req.columns else pd.Series([], dtype=bool)
        planned_mask = site_req["request_type"].eq("Planned requests") if "request_type" in site_req.columns else pd.Series([], dtype=bool)
        validated_mask = site_req["decision"].isin(["Validate all subventions", "Validate some subventions"]) if "decision" in site_req.columns else pd.Series([], dtype=bool)
        won_mask = site_req["subsidy_outcome"].eq("Won") if "subsidy_outcome" in site_req.columns else pd.Series([], dtype=bool)
        in_progress_mask = site_req["decision"].isna() if "decision" in site_req.columns else pd.Series([], dtype=bool)

        est_sum = float(site_req["estimated_budget"].sum()) if "estimated_budget" in site_req.columns else 0.0
        awd_sum = float(site_req["amount_awarded"].sum()) if "amount_awarded" in site_req.columns else 0.0

        completed = int((site_ap["status"] == "Completed").sum()) if "status" in site_ap.columns else 0
        late = int((site_ap["status"] == "Late").sum()) if "status" in site_ap.columns else 0

        rows.append({
            "Site": site,
            "Demandes spontanées": int(spontaneous_mask.sum()),
            "Montants en cours (€)": float(site_req.loc[in_progress_mask, "estimated_budget"].sum()) if "estimated_budget" in site_req.columns else 0.0,
            "Demandes validées": int(validated_mask.sum()),
            "Montants obtenus (€)": awd_sum,
            "Actions réalisées": completed,
            "Actions en retard": late,
            "Plant Requests": int(planned_mask.sum()),
            "Success Rate (validated)%": float((won_mask & validated_mask).sum() / max(int(validated_mask.sum()), 1) * 100),
            "Percent Positive Answers%": float(validated_mask.sum() / max(total, 1) * 100),
            "Impact on Competitiveness%": float((awd_sum / est_sum * 100) if est_sum > 0 else 0.0),
        })

    return pd.DataFrame(rows)


def calculate_kpis_by_project(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    df["estimated_budget"] = pd.to_numeric(df.get("estimated_budget"), errors="coerce").fillna(0)
    df["amount_awarded"] = pd.to_numeric(df.get("amount_awarded"), errors="coerce").fillna(0)

    for col in ["request_type", "decision", "subsidy_outcome", "site_location"]:
        if col in df.columns:
            df[col] = _clean_text(df[col])
    for col in ["project_title", "plant", "status"]:
        if col in ap.columns:
            ap[col] = _clean_text(ap[col])

    projects = sorted(ap.get("project_title", pd.Series([])).dropna().unique())
    rows: List[Dict] = []

    for project in projects:
        proj_actions = ap[ap["project_title"] == project] if "project_title" in ap.columns else ap.iloc[0:0]
        site = proj_actions["plant"].mode()[0] if ("plant" in proj_actions.columns and not proj_actions["plant"].mode().empty) else "N/A"
        proj_reqs = df[df["site_location"] == site] if (site != "N/A" and "site_location" in df.columns) else df.iloc[0:0]

        total = len(proj_reqs)
        spontaneous_mask = proj_reqs["request_type"].eq("Spontaneous requests") if "request_type" in proj_reqs.columns else pd.Series([], dtype=bool)
        planned_mask = proj_reqs["request_type"].eq("Planned requests") if "request_type" in proj_reqs.columns else pd.Series([], dtype=bool)
        validated_mask = proj_reqs["decision"].isin(["Validate all subventions", "Validate some subventions"]) if "decision" in proj_reqs.columns else pd.Series([], dtype=bool)
        won_mask = proj_reqs["subsidy_outcome"].eq("Won") if "subsidy_outcome" in proj_reqs.columns else pd.Series([], dtype=bool)
        in_progress_mask = proj_reqs["decision"].isna() if "decision" in proj_reqs.columns else pd.Series([], dtype=bool)

        est_sum = float(proj_reqs["estimated_budget"].sum()) if "estimated_budget" in proj_reqs.columns else 0.0
        awd_sum = float(proj_reqs["amount_awarded"].sum()) if "amount_awarded" in proj_reqs.columns else 0.0

        completed = int((proj_actions["status"] == "Completed").sum()) if "status" in proj_actions.columns else 0
        late = int((proj_actions["status"] == "Late").sum()) if "status" in proj_actions.columns else 0

        rows.append({
            "Projet": project,
            "Site": site,
            "Demandes spontanées": int(spontaneous_mask.sum()),
            "Montants en cours (€)": float(proj_reqs.loc[in_progress_mask, "estimated_budget"].sum()) if "estimated_budget" in proj_reqs.columns else 0.0,
            "Demandes validées": int(validated_mask.sum()),
            "Montants obtenus (€)": awd_sum,
            "Actions réalisées": completed,
            "Actions en retard": late,
            "Plant Requests": int(planned_mask.sum()),
            "Success Rate (validated)%": float((won_mask & validated_mask).sum() / max(int(validated_mask.sum()), 1) * 100),
            "Percent Positive Answers%": float(validated_mask.sum() / max(total, 1) * 100),
            "Impact on Competitiveness%": float((awd_sum / est_sum * 100) if est_sum > 0 else 0.0),
        })

    return pd.DataFrame(rows)


def get_detailed_data_for_visualization(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    return {
        "requests_summary": subsidy_requests[["name", "site_location", "request_type", "decision", "estimated_budget", "amount_awarded", "date_creation"]].copy(),
        "actions_summary": action_plan[["project_title", "plant", "status", "action_type", "initiation_date", "due_date", "expected_gain"]].copy(),
    }


# =============================================================================
# API routes
# =============================================================================
@app.route("/")
def home():
    return jsonify({
        "message": "Subsidy KPI API + Auto Sync (Cron every 15 min) + Weekly Reminders (Cron)",
        "timezone": REMINDER_TIMEZONE,
        "background_jobs": {
            "ENABLE_BACKGROUND_JOBS": ENABLE_BACKGROUND_JOBS,
            "ENABLE_POLLING_SYNC": ENABLE_POLLING_SYNC,
            "ENABLE_EMAIL_SCHEDULER": ENABLE_EMAIL_SCHEDULER,
            "ENABLE_DB_ADVISORY_LOCK": ENABLE_DB_ADVISORY_LOCK,
        }
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync", methods=["GET", "POST"])
def sync_now():
    try:
        s = perform_full_sync()
        return jsonify({"success": True, "data": s}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/send-reminders", methods=["GET", "POST"])
def trigger_reminders():
    try:
        result = send_late_action_reminders()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/scheduler-status", methods=["GET"])
def scheduler_status():
    global _scheduler_instance
    if not _scheduler_instance:
        return jsonify({"running": False, "reason": "no_scheduler_instance"}), 200

    jobs = []
    for j in _scheduler_instance.get_jobs():
        jobs.append({
            "id": j.id,
            "name": j.name,
            "trigger": str(j.trigger),
            "next_run_time": j.next_run_time.isoformat() if j.next_run_time else None,
        })

    return jsonify({
        "running": bool(_scheduler_instance.running),
        "timezone": REMINDER_TIMEZONE,
        "server_utc_now": utc_now().isoformat(),
        "config": {
            "REMINDER_DAY_OF_WEEK": REMINDER_DAY_OF_WEEK,
            "REMINDER_HOUR": REMINDER_HOUR,
            "REMINDER_MINUTE": REMINDER_MINUTE,
            "ENABLE_BACKGROUND_JOBS": ENABLE_BACKGROUND_JOBS,
            "ENABLE_POLLING_SYNC": ENABLE_POLLING_SYNC,
            "ENABLE_EMAIL_SCHEDULER": ENABLE_EMAIL_SCHEDULER,
            "ENABLE_DB_ADVISORY_LOCK": ENABLE_DB_ADVISORY_LOCK,
        },
        "jobs": jobs,
    }), 200


@app.route("/api/kpis/global", methods=["GET"])
def get_global_kpis():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        kpis = calculate_global_kpis(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kpis/by-site", methods=["GET"])
def get_kpis_by_site():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        kpis_df = calculate_kpis_by_site(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis_df.to_dict(orient="records")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kpis/by-project", methods=["GET"])
def get_kpis_by_project():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        kpis_df = calculate_kpis_by_project(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis_df.to_dict(orient="records")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kpis/all", methods=["GET"])
def get_all_kpis():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        global_kpis = calculate_global_kpis(subsidy_requests, action_plan)
        kpis_by_site = calculate_kpis_by_site(subsidy_requests, action_plan)
        kpis_by_project = calculate_kpis_by_project(subsidy_requests, action_plan)

        return jsonify({
            "success": True,
            "data": {
                "global": global_kpis,
                "by_site": kpis_by_site.to_dict(orient="records"),
                "by_project": kpis_by_project.to_dict(orient="records"),
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/data/requests", methods=["GET"])
def get_requests_data():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        conn.close()

        data = json.loads(subsidy_requests.to_json(orient="records", date_format="iso"))
        return jsonify({"success": True, "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/data/actions", methods=["GET"])
def get_actions_data():
    try:
        conn = get_db_connection()
        action_plan = get_action_plan_data(conn)
        conn.close()

        data = json.loads(action_plan.to_json(orient="records", date_format="iso"))
        return jsonify({"success": True, "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/data/visualization", methods=["GET"])
def get_visualization_data():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        viz_data = get_detailed_data_for_visualization(subsidy_requests, action_plan)
        return jsonify({
            "success": True,
            "data": {
                "requests_summary": json.loads(viz_data["requests_summary"].to_json(orient="records", date_format="iso")),
                "actions_summary": json.loads(viz_data["actions_summary"].to_json(orient="records", date_format="iso")),
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.errorhandler(404)
def not_found(_):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(_):
    return jsonify({"success": False, "error": "Internal server error"}), 500


# =============================================================================
# Background jobs bootstrap (WSGI/Azure compatible)
# =============================================================================
_background_started = False
_scheduler_instance: Optional[BackgroundScheduler] = None


def start_scheduler():
    """
    Runs BOTH (inside APScheduler):
    - Weekly reminders (Cron)
    - Auto-sync every 15 minutes (Cron: minute="*/15")
    """
    if not ENABLE_EMAIL_SCHEDULER and not ENABLE_POLLING_SYNC:
        print("[scheduler] Disabled (both ENABLE_EMAIL_SCHEDULER and ENABLE_POLLING_SYNC are false)", flush=True)
        return None

    tz = REMINDER_TZINFO
    scheduler = BackgroundScheduler(timezone=tz) if tz else BackgroundScheduler()

    # Weekly reminders
    if ENABLE_EMAIL_SCHEDULER:
        reminder_trigger = CronTrigger(
            day_of_week=REMINDER_DAY_OF_WEEK,
            hour=REMINDER_HOUR,
            minute=REMINDER_MINUTE,
            timezone=tz,
        )
        scheduler.add_job(
            func=_scheduled_reminders_job,
            trigger=reminder_trigger,
            id="weekly_late_action_check",
            name="Weekly Late Action Reminder",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
            max_instances=1,
        )
        print(
            f"[scheduler] Weekly reminder scheduled: {REMINDER_DAY_OF_WEEK} {REMINDER_HOUR:02d}:{REMINDER_MINUTE:02d} ({REMINDER_TIMEZONE})",
            flush=True
        )

    # Auto-sync every 15 minutes (00, 15, 30, 45)
    if ENABLE_POLLING_SYNC:
        sync_trigger = CronTrigger(
            minute="*/15",
            timezone=tz,
        )
        scheduler.add_job(
            func=_scheduled_sync_job,
            trigger=sync_trigger,
            id="auto_sync",
            name="Auto Sync (every 15 min)",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=3600,
            max_instances=1,
        )
        print(f"[scheduler] Auto-sync scheduled every 15 minutes ({REMINDER_TIMEZONE})", flush=True)

    scheduler.start()
    print("[scheduler] Started", flush=True)
    return scheduler


def start_background_services_once():
    global _background_started, _scheduler_instance

    if not ENABLE_BACKGROUND_JOBS:
        print("[jobs] Disabled (ENABLE_BACKGROUND_JOBS=false)", flush=True)
        return
    if _background_started:
        return

    # Local debug reloader guard
    if _env_bool("USE_RELOADER", False) and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        print("[jobs] Reloader parent detected: not starting background jobs.", flush=True)
        return

    _background_started = True
    print("[jobs] Starting background services...", flush=True)

    _scheduler_instance = start_scheduler()
    if _scheduler_instance:
        atexit.register(lambda: _scheduler_instance.shutdown() if _scheduler_instance else None)

    print("[jobs] Background services started.", flush=True)


# Start jobs at import time (Azure WSGI will import this module)
start_background_services_once()


# =============================================================================
# Run locally only
# =============================================================================
if __name__ == "__main__":
    DEBUG = _env_bool("FLASK_DEBUG", _env_bool("DEBUG", True))
    USE_RELOADER = _env_bool("USE_RELOADER", DEBUG)
    HOST = _first_env(["FLASK_RUN_HOST"], "0.0.0.0")
    PORT = int(_first_env(["PORT", "FLASK_RUN_PORT"], "5000"))

    print(f"[app] Starting Flask (debug={DEBUG}, reloader={USE_RELOADER})", flush=True)
    app.run(debug=DEBUG, host=HOST, port=PORT, use_reloader=USE_RELOADER)
