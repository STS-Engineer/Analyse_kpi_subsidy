"""
Flask application combining KPI metrics endpoints and Monday.com synchronization.

This module defines a single Flask app that exposes both the KPI API and
endpoints to synchronise data from monday.com boards into a PostgreSQL
database.  It merges the functionality of two previously separate
applications:

* The KPI API calculates various key performance indicators based on the
  `subsidy_requests` and `subsidy_action_plan` tables.  It exposes
  endpoints under `/api/kpis/` for global, by-site and by-project
  metrics, and endpoints under `/api/data/` that return raw data for
  inspection or visualisation.

* The Monday sync API listens for webhook events from monday.com and
  provides a `/sync` endpoint to manually fetch all items from two
  configured boards.  It stores each item in its respective table,
  inserting new rows or updating existing ones based on the item's
  numeric ID.  The webhook endpoint (`/webhook`) handles creation,
  updates and deletion of individual items as they happen.

The unified app is configured via environment variables.  For database
connectivity, it respects the same variables as the original KPI API:

* **PGHOST** – PostgreSQL server host
* **PGDATABASE** – database name
* **PGUSER** – database user
* **PGPASSWORD** – database user password
* **PGPORT** – database port (default 5432)
* **PGSSLMODE** – PostgreSQL SSL mode (default "require")

For monday.com integration the following variables are used:

* **MONDAY_API_KEY** – API token used to authenticate API requests.
* **MONDAY_BOARD_REQUESTS_ID** – ID of the board containing subsidy
  request items (default 9612741617).
* **MONDAY_BOARD_ACTIONS_ID** – ID of the board containing action
  plan items (default 9366723818).
* **DB_TABLE_REQUESTS** – name of the table for request items
  (default "subsidy_requests").
* **DB_TABLE_ACTIONS** – name of the table for action items
  (default "subsidy_action_plan").

If psycopg2 has been compiled without SSL support, SSL may be disabled
by setting the PGSSLMODE environment variable appropriately.  The
defaults match the provided examples.
"""

import json
import os
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None  # type: ignore


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

# Database configuration derived from environment variables.  The
# environment variables allow overriding of host, database name, user
# credentials, port, and SSL mode.  Defaults are provided to allow
# running in development without additional configuration.
DB_CONFIG = {
    "host": os.getenv("PGHOST", "avo-adb-002.postgres.database.azure.com"),
    "database": os.getenv("PGDATABASE", "Subsidy_DB"),
    "user": os.getenv("PGUSER", "administrationSTS"),
    "password": os.getenv("PGPASSWORD", "St$@0987"),
    "port": int(os.getenv("PGPORT", "5432")),
    # psycopg2 accepts the sslmode parameter; default to 'require' to
    # ensure secure connections unless explicitly overridden.
    "sslmode": os.getenv("PGSSLMODE", "require"),
}

# Monday.com integration configuration.  These variables can be set
# externally to configure the API key and board IDs.
MONDAY_API_KEY: Optional[str] = os.environ.get("MONDAY_API_KEY")
MONDAY_BOARD_REQUESTS_ID: int = int(os.environ.get("MONDAY_BOARD_REQUESTS_ID", "9612741617"))
MONDAY_BOARD_ACTIONS_ID: int = int(os.environ.get("MONDAY_BOARD_ACTIONS_ID", "9366723818"))
DB_TABLE_REQUESTS: str = os.environ.get("DB_TABLE_REQUESTS", "subsidy_requests")
DB_TABLE_ACTIONS: str = os.environ.get("DB_TABLE_ACTIONS", "subsidy_action_plan")

# Interval in seconds to run automatic synchronisation.  If this value
# is set and positive, the application will start a background thread
# that periodically synchronises the configured monday.com boards with
# the database.  You can disable the periodic sync by setting
# `MONDAY_SYNC_INTERVAL` to 0 or leaving it unset.  A reasonable
# default of 300 seconds (5 minutes) is used if unspecified.
MONDAY_SYNC_INTERVAL: int = int(os.environ.get("MONDAY_SYNC_INTERVAL", "0"))


# ----------------------------------------------------------------------------
# Application setup
# ----------------------------------------------------------------------------

app = Flask(__name__)
cors = CORS(app)


# ----------------------------------------------------------------------------
# Utility classes and functions
# ----------------------------------------------------------------------------

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to serialise date and datetime objects."""

    def default(self, obj):  # type: ignore[override]
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def get_db_connection():
    """Establish a new database connection.

    Returns a new connection to the PostgreSQL database using the
    configured settings.  A runtime error is raised if psycopg2 is not
    installed.
    """
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is required for database connectivity but is not installed. "
            "Install it with 'pip install psycopg2-binary'."
        )
    return psycopg2.connect(**DB_CONFIG)


def get_subsidy_requests_data(connection) -> pd.DataFrame:
    """Retrieve subsidy request data from the database."""
    query = """
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
    FROM {};
    """.format(DB_TABLE_REQUESTS)
    return pd.read_sql_query(query, connection)


def get_action_plan_data(connection) -> pd.DataFrame:
    """Retrieve action plan data from the database."""
    query = """
    SELECT 
        id,
        name,
        action_id,
        project_title,
        status,
        action_type,
        initiation_date,
        due_date,
        plant,
        expected_gain
    FROM {};
    """.format(DB_TABLE_ACTIONS)
    return pd.read_sql_query(query, connection)


def _clean_text(series: pd.Series) -> pd.Series:
    """Normalise text by stripping whitespace and converting to string."""
    return series.astype(str).str.strip()


def calculate_global_kpis(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict[str, float]:
    """Calculate high-level KPI metrics across all subsidy requests and actions."""
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    # Normalise numeric columns
    df['estimated_budget'] = pd.to_numeric(df['estimated_budget'], errors='coerce').fillna(0)
    df['amount_awarded'] = pd.to_numeric(df['amount_awarded'], errors='coerce').fillna(0)

    # Clean relevant text columns
    for col in ['request_type', 'decision', 'subsidy_outcome', 'site_location']:
        if col in df.columns:
            df[col] = _clean_text(df[col])

    # Define boolean masks for counting
    total_requests = len(df)
    spontaneous_mask = df['request_type'].eq('Spontaneous requests')
    planned_mask = df['request_type'].eq('Planned requests')
    validated_mask = df['decision'].isin(['Validate all subventions', 'Validate some subventions'])
    any_answer_mask = df['decision'].notna()
    won_mask = df['subsidy_outcome'].eq('Won')
    in_progress_mask = df['decision'].isna()

    kpis: Dict[str, float] = {}
    kpis['nombre_demandes_spontanees'] = float(spontaneous_mask.sum())
    kpis['montants_en_cours'] = float(df.loc[in_progress_mask, 'estimated_budget'].sum())
    kpis['nombre_demandes_validees'] = float(validated_mask.sum())
    kpis['montants_obtenus'] = float(df['amount_awarded'].sum())

    # Action-related KPIs
    kpis['actions_realisees'] = float((action_plan['status'] == 'Completed').sum())
    kpis['actions_en_retard'] = float((action_plan['status'] == 'Late').sum())

    # Additional KPIs requested
    kpis['plant_requests'] = float(planned_mask.sum())

    denom_validated = max(int(validated_mask.sum()), 1)
    kpis['success_rate_validated'] = float((won_mask & validated_mask).sum() / denom_validated * 100)
    # optional alternative measure left for completeness
    kpis['success_rate_submitted_alt'] = float(validated_mask.sum() / max(total_requests, 1) * 100)

    total_est = float(df['estimated_budget'].sum())
    total_awd = float(df['amount_awarded'].sum())
    kpis['impact_competitiveness'] = float((total_awd / total_est * 100) if total_est > 0 else 0.0)

    kpis['percent_positive_answers'] = float(validated_mask.sum() / max(total_requests, 1) * 100)
    kpis['percent_any_answer_alt'] = float(any_answer_mask.sum() / max(total_requests, 1) * 100)
    return kpis


def calculate_kpis_by_site(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    """Calculate KPI metrics grouped by site location."""
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    # Normalise numeric and text columns
    df['estimated_budget'] = pd.to_numeric(df['estimated_budget'], errors='coerce').fillna(0)
    df['amount_awarded'] = pd.to_numeric(df['amount_awarded'], errors='coerce').fillna(0)
    for col in ['request_type', 'decision', 'subsidy_outcome', 'site_location']:
        if col in df.columns:
            df[col] = _clean_text(df[col])
    if 'plant' in ap.columns:
        ap['plant'] = _clean_text(ap['plant'])

    # Determine unique sites across both tables
    sites = sorted(set(df['site_location'].dropna().unique()) | set(ap['plant'].dropna().unique()))
    rows: List[Dict[str, float]] = []

    for site in sites:
        site_req = df[df['site_location'] == site]
        site_ap = ap[ap['plant'] == site]

        total = len(site_req)
        spontaneous_mask = site_req['request_type'].eq('Spontaneous requests')
        planned_mask = site_req['request_type'].eq('Planned requests')
        validated_mask = site_req['decision'].isin(['Validate all subventions', 'Validate some subventions'])
        won_mask = site_req['subsidy_outcome'].eq('Won')
        in_progress_mask = site_req['decision'].isna()

        est_sum = float(site_req['estimated_budget'].sum())
        awd_sum = float(site_req['amount_awarded'].sum())

        completed = float((site_ap['status'] == 'Completed').sum())
        late = float((site_ap['status'] == 'Late').sum())

        row: Dict[str, float] = {
            'Site': site,
            'Demandes spontanées': float(spontaneous_mask.sum()),
            'Montants en cours (€)': float(site_req.loc[in_progress_mask, 'estimated_budget'].sum()),
            'Total demandes spontanées': float(spontaneous_mask.sum()),
            'Demandes validées': float(validated_mask.sum()),
            'Montants obtenus (€)': awd_sum,
            'Actions réalisées': completed,
            'Actions en retard': late,
            'Plant Requests': float(planned_mask.sum()),
            'Success Rate (validated)%': float((won_mask & validated_mask).sum() / max(int(validated_mask.sum()), 1) * 100),
            'Percent Positive Answers%': float(validated_mask.sum() / max(total, 1) * 100),
            'Impact on Competitiveness%': float((awd_sum / est_sum * 100) if est_sum > 0 else 0.0),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def calculate_kpis_by_project(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    """Calculate KPI metrics grouped by project (title) as defined in the action plan."""
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    df['estimated_budget'] = pd.to_numeric(df['estimated_budget'], errors='coerce').fillna(0)
    df['amount_awarded'] = pd.to_numeric(df['amount_awarded'], errors='coerce').fillna(0)

    for col in ['request_type', 'decision', 'subsidy_outcome', 'site_location']:
        if col in df.columns:
            df[col] = _clean_text(df[col])
    for col in ['project_title', 'plant', 'status']:
        if col in ap.columns:
            ap[col] = _clean_text(ap[col])

    projects = sorted(ap['project_title'].dropna().unique())
    rows: List[Dict[str, float]] = []

    for project in projects:
        proj_actions = ap[ap['project_title'] == project]
        site = proj_actions['plant'].mode()[0] if not proj_actions['plant'].mode().empty else 'N/A'
        proj_reqs = df[df['site_location'] == site] if site != 'N/A' else df.iloc[0:0]

        total = len(proj_reqs)
        spontaneous_mask = proj_reqs['request_type'].eq('Spontaneous requests')
        planned_mask = proj_reqs['request_type'].eq('Planned requests')
        validated_mask = proj_reqs['decision'].isin(['Validate all subventions', 'Validate some subventions'])
        won_mask = proj_reqs['subsidy_outcome'].eq('Won')
        in_progress_mask = proj_reqs['decision'].isna()

        est_sum = float(proj_reqs['estimated_budget'].sum())
        awd_sum = float(proj_reqs['amount_awarded'].sum())

        completed = float((proj_actions['status'] == 'Completed').sum())
        late = float((proj_actions['status'] == 'Late').sum())

        row: Dict[str, float] = {
            'Projet': project,
            'Site': site,
            'Demandes spontanées': float(spontaneous_mask.sum()),
            'Montants en cours (€)': float(proj_reqs.loc[in_progress_mask, 'estimated_budget'].sum()),
            'Total demandes spontanées': float(spontaneous_mask.sum()),
            'Demandes validées': float(validated_mask.sum()),
            'Montants obtenus (€)': awd_sum,
            'Actions réalisées': completed,
            'Actions en retard': late,
            'Plant Requests': float(planned_mask.sum()),
            'Success Rate (validated)%': float((won_mask & validated_mask).sum() / max(int(validated_mask.sum()), 1) * 100),
            'Percent Positive Answers%': float(validated_mask.sum() / max(total, 1) * 100),
            'Impact on Competitiveness%': float((awd_sum / est_sum * 100) if est_sum > 0 else 0.0),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def get_detailed_data_for_visualization(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Return simplified data structures for charting or front-end visualisation."""
    viz_data: Dict[str, pd.DataFrame] = {}
    viz_data['requests_summary'] = subsidy_requests[[
        'name', 'site_location', 'request_type', 'decision',
        'estimated_budget', 'amount_awarded', 'date_creation'
    ]].copy()
    viz_data['actions_summary'] = action_plan[[
        'project_title', 'plant', 'status', 'action_type',
        'initiation_date', 'due_date', 'expected_gain'
    ]].copy()
    return viz_data


# ----------------------------------------------------------------------------
# Monday.com helper functions
# ----------------------------------------------------------------------------

def query_monday_items(api_key: str, board_id: int) -> List[Dict[str, any]]:
    """Fetch all items from a monday.com board using the GraphQL API.

    Returns a list of items with their column values.  Raises a
    RuntimeError if the API call fails or returns errors.
    """
    # GraphQL query: ask for items and their column values on the board
    query = f"""
    {{
      boards(ids: {board_id}) {{
        items_page(limit: 100) {{
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
    api_url = "https://api.monday.com/v2"
    headers = {"Authorization": api_key}
    data = {"query": query}
    response = requests.post(api_url, json=data, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(
            f"monday.com API call failed with status {response.status_code}: {response.text}"
        )
    result = response.json()
    if "errors" in result:
        raise RuntimeError(f"monday.com API returned errors: {result['errors']}")
    boards = result.get("data", {}).get("boards", [])
    if not boards:
        return []
    return boards[0].get("items_page", {}).get("items", [])


def query_monday_item(api_key: str, board_id: int, item_id: int) -> Optional[Dict[str, any]]:
    """Fetch a single item by its ID from a monday.com board."""
    query = f"""
    {{
      items(ids: [{item_id}]) {{
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
    """
    api_url = "https://api.monday.com/v2"
    headers = {"Authorization": api_key}
    data = {"query": query}
    response = requests.post(api_url, json=data, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(
            f"monday.com API call failed with status {response.status_code}: {response.text}"
        )
    result = response.json()
    if "errors" in result:
        raise RuntimeError(f"monday.com API returned errors: {result['errors']}")
    items = result.get("data", {}).get("items", [])
    return items[0] if items else None


def upsert_request_item(
    cursor: "psycopg2.extensions.cursor", table: str, item: Dict[str, any]
) -> str:
    """Insert or update a row in the requests table.

    Uses the numeric item ID as the primary key.  If an existing row
    with the same ID exists, its name and column_values fields are
    updated.  Otherwise, a new row is inserted.
    """
    item_id = int(item.get("id"))
    name = item.get("name", "")
    values_dict: Dict[str, Optional[str]] = {}
    for cv in item.get("column_values", []):
        col_id = cv.get("id")
        text = cv.get("text")
        values_dict[col_id] = text

    # Determine whether the row exists
    cursor.execute(f"SELECT 1 FROM {table} WHERE id = %s", (item_id,))
    exists = cursor.fetchone()
    if exists is None:
        cursor.execute(
            f"INSERT INTO {table} (id, name, column_values) VALUES (%s, %s, %s)",
            (item_id, name, Json(values_dict)),
        )
        return "insert"
    else:
        cursor.execute(
            f"UPDATE {table} SET name = %s, column_values = %s WHERE id = %s",
            (name, Json(values_dict), item_id),
        )
        return "update"


def upsert_action_item(
    cursor: "psycopg2.extensions.cursor", table: str, item: Dict[str, any]
) -> str:
    """Insert or update a row in the action plan table.

    Stores the value of the "AI subsidy assistant" column, if present,
    in an `action_detail` column.  The other column values are stored
    as JSON in the `column_values` column.  Rows are matched using the
    item ID as the primary key.
    """
    item_id = int(item.get("id"))
    name = item.get("name", "")
    action_detail: Optional[str] = None
    values_dict: Dict[str, Optional[str]] = {}
    for cv in item.get("column_values", []):
        col_id = cv.get("id")
        text = cv.get("text")
        values_dict[col_id] = text
        column_info = cv.get("column") or {}
        title = column_info.get("title")
        if title and title.strip().lower() == "ai subsidy assistant".lower():
            action_detail = text

    # Determine whether the row exists
    cursor.execute(f"SELECT 1 FROM {table} WHERE id = %s", (item_id,))
    exists = cursor.fetchone()
    if exists is None:
        cursor.execute(
            f"INSERT INTO {table} (id, name, action_detail, column_values) VALUES (%s, %s, %s, %s)",
            (item_id, name, action_detail, Json(values_dict)),
        )
        return "insert"
    else:
        cursor.execute(
            f"UPDATE {table} SET name = %s, action_detail = %s, column_values = %s WHERE id = %s",
            (name, action_detail, Json(values_dict), item_id),
        )
        return "update"

# ----------------------------------------------------------------------------
# Periodic synchronisation
# ----------------------------------------------------------------------------

def perform_sync_and_cleanup() -> Dict[str, Dict[str, int]]:
    """Synchronise the monday.com boards with the database, including deletions.

    This function performs the following steps:

    1. Fetch all items from the configured requests and actions boards.
    2. Upsert each item into the corresponding table (insert new rows or
       update existing ones).
    3. Identify any items that are present in the database but no longer
       exist on the board and delete those rows.

    A summary dictionary containing counts of inserts, updates and
    deletions for each table is returned.  Any exceptions raised
    during the process are propagated to the caller.
    """
    api_key = MONDAY_API_KEY or os.environ.get("MONDAY_API_KEY")
    if not api_key:
        raise RuntimeError("MONDAY_API_KEY env var not set")

    board_requests_id = MONDAY_BOARD_REQUESTS_ID
    board_actions_id = MONDAY_BOARD_ACTIONS_ID
    table_requests = DB_TABLE_REQUESTS
    table_actions = DB_TABLE_ACTIONS

    # Fetch all items from both boards
    items_requests = query_monday_items(api_key, board_requests_id)
    items_actions = query_monday_items(api_key, board_actions_id)

    summary: Dict[str, Dict[str, int]] = {
        "requests": {"inserted": 0, "updated": 0, "deleted": 0},
        "actions": {"inserted": 0, "updated": 0, "deleted": 0},
    }

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Synchronise requests items
                new_ids_requests = set(int(item.get("id")) for item in items_requests)
                # Upsert items
                for item in items_requests:
                    action = upsert_request_item(cur, table_requests, item)
                    summary["requests"]["inserted" if action == "insert" else "updated"] += 1
                # Delete rows that no longer exist on the board
                cur.execute(f"SELECT id FROM {table_requests}")
                existing_ids_requests = {row[0] for row in cur.fetchall()}
                ids_to_delete = existing_ids_requests - new_ids_requests
                for old_id in ids_to_delete:
                    cur.execute(f"DELETE FROM {table_requests} WHERE id = %s", (old_id,))
                    summary["requests"]["deleted"] += 1

                # Synchronise actions items
                new_ids_actions = set(int(item.get("id")) for item in items_actions)
                for item in items_actions:
                    action2 = upsert_action_item(cur, table_actions, item)
                    summary["actions"]["inserted" if action2 == "insert" else "updated"] += 1
                cur.execute(f"SELECT id FROM {table_actions}")
                existing_ids_actions = {row[0] for row in cur.fetchall()}
                ids_to_delete_actions = existing_ids_actions - new_ids_actions
                for old_id in ids_to_delete_actions:
                    cur.execute(f"DELETE FROM {table_actions} WHERE id = %s", (old_id,))
                    summary["actions"]["deleted"] += 1
    finally:
        conn.close()
    return summary

# Background synchronisation thread
def _sync_thread() -> None:
    """Internal function run in a separate thread to perform periodic syncs."""
    # Import here to avoid circular imports when this module is imported by others
    import time
    while True:
        try:
            perform_sync_and_cleanup()
        except Exception as exc:
            # Print the exception rather than raising it, to keep the thread alive.
            print(f"[sync_thread] Error during sync: {exc}")
        # Sleep for the configured interval.  If interval is 0 or negative,
        # exit the loop (which stops the thread).
        interval = MONDAY_SYNC_INTERVAL
        if interval <= 0:
            break
        time.sleep(interval)


def start_periodic_sync() -> None:
    """Start the periodic sync thread if MONDAY_SYNC_INTERVAL is positive."""
    if MONDAY_SYNC_INTERVAL > 0:
        import threading
        t = threading.Thread(target=_sync_thread, daemon=True)
        t.start()


# ----------------------------------------------------------------------------
# API endpoints
# ----------------------------------------------------------------------------

@app.route('/')
def home():
    """Return a description of the API and available endpoints."""
    return jsonify({
        "message": "Subsidy KPI and Monday Sync API",
        "version": "1.2",
        "endpoints": {
            "/api/kpis/global": "GET – Global KPI metrics",
            "/api/kpis/by-site": "GET – KPI metrics grouped by site",
            "/api/kpis/by-project": "GET – KPI metrics grouped by project",
            "/api/kpis/all": "GET – All KPI metrics combined",
            "/api/data/requests": "GET – Raw subsidy requests data",
            "/api/data/actions": "GET – Raw action plan data",
            "/api/data/visualization": "GET – Simplified data for charts",
            "/health": "GET – Health check for the service",
            "/sync": "GET/POST – Manually synchronise monday.com boards",
            "/webhook": "POST – Receive webhook events from monday.com"
        }
    })


@app.route('/api/kpis/global', methods=['GET'])
def get_global_kpis():
    """Endpoint to compute global KPI metrics."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        kpis = calculate_global_kpis(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis})
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/kpis/by-site', methods=['GET'])
def get_kpis_by_site_route():
    """Endpoint to compute KPI metrics by site."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        kpis_df = calculate_kpis_by_site(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis_df.to_dict(orient='records')})
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/kpis/by-project', methods=['GET'])
def get_kpis_by_project_route():
    """Endpoint to compute KPI metrics by project."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        kpis_df = calculate_kpis_by_project(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis_df.to_dict(orient='records')})
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/kpis/all', methods=['GET'])
def get_all_kpis_route():
    """Endpoint to return all KPI metrics (global, by site and by project)."""
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
                "by_site": kpis_by_site.to_dict(orient='records'),
                "by_project": kpis_by_project.to_dict(orient='records'),
            },
        })
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/data/requests', methods=['GET'])
def get_requests_data():
    """Endpoint to fetch raw subsidy request records."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        conn.close()
        data = json.loads(subsidy_requests.to_json(orient='records', date_format='iso'))
        return jsonify({"success": True, "count": len(data), "data": data})
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/data/actions', methods=['GET'])
def get_actions_data():
    """Endpoint to fetch raw action plan records."""
    try:
        conn = get_db_connection()
        action_plan = get_action_plan_data(conn)
        conn.close()
        data = json.loads(action_plan.to_json(orient='records', date_format='iso'))
        return jsonify({"success": True, "count": len(data), "data": data})
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/data/visualization', methods=['GET'])
def get_visualization_data():
    """Endpoint to return simplified data for visualisation purposes."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        viz_data = get_detailed_data_for_visualization(subsidy_requests, action_plan)
        return jsonify({
            "success": True,
            "data": {
                "requests_summary": json.loads(viz_data['requests_summary'].to_json(orient='records', date_format='iso')),
                "actions_summary": json.loads(viz_data['actions_summary'].to_json(orient='records', date_format='iso')),
            },
        })
    except Exception as e:  # pylint: disable=broad-except
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "ok"}), 200


@app.route('/sync', methods=['GET', 'POST'])
def sync_boards():
    """Synchronise the configured monday.com boards with the database."""
    api_key = MONDAY_API_KEY or os.environ.get("MONDAY_API_KEY")
    if not api_key:
        return jsonify({"error": "MONDAY_API_KEY env var not set"}), 500

    board_requests_id = MONDAY_BOARD_REQUESTS_ID
    board_actions_id = MONDAY_BOARD_ACTIONS_ID
    table_requests = DB_TABLE_REQUESTS
    table_actions = DB_TABLE_ACTIONS

    # Fetch all items from both boards
    try:
        items_requests = query_monday_items(api_key, board_requests_id)
        items_actions = query_monday_items(api_key, board_actions_id)
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"error": f"Failed to fetch items: {exc}"}), 500

    inserted_requests = updated_requests = 0
    inserted_actions = updated_actions = 0
    try:
        conn = get_db_connection()
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"error": f"Database connection error: {exc}"}), 500

    try:
        with conn:
            with conn.cursor() as cur:
                # Upsert request items
                for item in items_requests:
                    action = upsert_request_item(cur, table_requests, item)
                    if action == "insert":
                        inserted_requests += 1
                    else:
                        updated_requests += 1
                # Upsert action items
                for item in items_actions:
                    action2 = upsert_action_item(cur, table_actions, item)
                    if action2 == "insert":
                        inserted_actions += 1
                    else:
                        updated_actions += 1
    finally:
        conn.close()

    return jsonify({
        "status": "success",
        "requests": {"inserted": inserted_requests, "updated": updated_requests},
        "actions": {"inserted": inserted_actions, "updated": updated_actions},
    }), 200


@app.route('/webhook', methods=['POST'])
def monday_webhook():
    """Handle incoming webhook events from monday.com."""
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "Invalid JSON"}), 400

    # Verification challenge
    if payload and isinstance(payload, dict) and "challenge" in payload:
        return jsonify({"challenge": payload["challenge"]}), 200

    event = payload.get("event") if payload else None
    if not event:
        return jsonify({"error": "Missing event in webhook payload"}), 400

    event_type = event.get("type")
    board_id = event.get("boardId") or event.get("board_id")
    item_id = event.get("pulseId") or event.get("itemId") or event.get("item_id")
    if not board_id or not item_id:
        return jsonify({"error": "Webhook event missing board or item ID"}), 400

    try:
        board_id_int = int(board_id)
        item_id_int = int(item_id)
    except ValueError:
        return jsonify({"error": "Invalid board or item ID"}), 400

    api_key = MONDAY_API_KEY or os.environ.get("MONDAY_API_KEY")
    if not api_key:
        return jsonify({"error": "MONDAY_API_KEY env var not set"}), 500

    board_requests_id = MONDAY_BOARD_REQUESTS_ID
    board_actions_id = MONDAY_BOARD_ACTIONS_ID
    target: Optional[str] = None
    if board_id_int == board_requests_id:
        target = "requests"
    elif board_id_int == board_actions_id:
        target = "actions"
    else:
        # Unrecognised board; ignore
        return jsonify({"status": "ignored", "reason": "Event from unconfigured board"}), 200

    table_requests = DB_TABLE_REQUESTS
    table_actions = DB_TABLE_ACTIONS
    table_name = table_requests if target == "requests" else table_actions

    # Deletion event: remove row from database
    if event_type == "delete_pulse":
        try:
            conn = get_db_connection()
        except Exception as exc:  # pylint: disable=broad-except
            return jsonify({"error": f"Database connection error: {exc}"}), 500
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (item_id_int,))
                    deleted = cur.rowcount > 0
        finally:
            conn.close()
        return jsonify({
            "status": "success",
            "target": target,
            "action": "delete",
            "deleted": deleted,
            "board_id": board_id_int,
            "item_id": item_id_int,
        }), 200

    # For creation or update events: fetch the item and upsert
    try:
        item = query_monday_item(api_key, board_id_int, item_id_int)
        if item is None:
            return jsonify({"status": "ignored", "reason": "Item not found"}), 200
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"error": f"Failed to fetch item: {exc}"}), 500

    try:
        conn = get_db_connection()
    except Exception as exc:  # pylint: disable=broad-except
        return jsonify({"error": f"Database connection error: {exc}"}), 500
    try:
        with conn:
            with conn.cursor() as cur:
                if target == "requests":
                    action = upsert_request_item(cur, table_name, item)
                else:
                    action = upsert_action_item(cur, table_name, item)
    finally:
        conn.close()
    return jsonify({
        "status": "success",
        "target": target,
        "action": action,
        "board_id": board_id_int,
        "item_id": item_id_int,
    }), 200


# ----------------------------------------------------------------------------
# Error handlers
# ----------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):  # pylint: disable=unused-argument
    return jsonify({"success": False, "error": "Internal server error"}), 500


if __name__ == '__main__':
    # When executed directly, run the Flask development server.  In
    # production, this application should be run by a WSGI server such
    # as Gunicorn.  The host is bound to 0.0.0.0 to allow external
    # connectivity from Docker or other containers.
    # Optionally start periodic synchronisation before running the server.
    start_periodic_sync()
    port = int(os.environ.get("FLASK_RUN_PORT", "5000"))
    app.run(debug=True, host='0.0.0.0', port=port)
