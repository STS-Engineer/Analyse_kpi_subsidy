from flask import Flask, jsonify
from flask_cors import CORS
import os
import psycopg2
import pandas as pd
from typing import Dict, List
from datetime import date, datetime
import json

# ============================================================================
# Flask & CORS
# ============================================================================
app = Flask(__name__)
CORS(app)

# ============================================================================
# DB CONFIG (use env vars for safety)
#   export PGHOST=...
#   export PGDATABASE=...
#   export PGUSER=...
#   export PGPASSWORD=...
# ============================================================================
DB_CONFIG = {
    "host": os.getenv("PGHOST", "avo-adb-002.postgres.database.azure.com"),
    "database": os.getenv("PGDATABASE", "Subsidy_DB"),
    "user": os.getenv("PGUSER", "administrationSTS"),
    "password": os.getenv("PGPASSWORD", "St$@0987"),
    "sslmode": os.getenv("PGSSLMODE", "require")
}

# ============================================================================
# Helpers
# ============================================================================
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Database connection error: {e}")

def get_subsidy_requests_data(connection) -> pd.DataFrame:
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
    FROM public.subsidy_requests
    """
    return pd.read_sql_query(query, connection)

def get_action_plan_data(connection) -> pd.DataFrame:
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
    FROM public.subsidy_action_plan
    """
    return pd.read_sql_query(query, connection)

# --- Small utility to be robust to casing/spacing in text columns
def _clean_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()

# ============================================================================
# KPI Calculations
#   Your KPIs added:
#   - plant_requests: request_type == 'Planned requests'
#   - success_rate_validated: Won among validated (%)
#   - percent_positive_answers: Validated among submitted (%)
#   - impact_competitiveness: sum(amount_awarded)/sum(estimated_budget) (%)
# ============================================================================
def calculate_global_kpis(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict:
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    # Normalize columns
    df['estimated_budget'] = pd.to_numeric(df['estimated_budget'], errors='coerce').fillna(0)
    df['amount_awarded']  = pd.to_numeric(df['amount_awarded'],  errors='coerce').fillna(0)

    # Clean text columns (be tolerant to case/whitespace)
    for col in ['request_type', 'decision', 'subsidy_outcome', 'site_location']:
        if col in df.columns:
            df[col] = _clean_text(df[col])

    # Masks & base sets
    total_requests = len(df)
    spontaneous_mask = df['request_type'].eq('Spontaneous requests')
    planned_mask     = df['request_type'].eq('Planned requests')          # <-- Plant Requests (your spec)
    validated_mask   = df['decision'].isin(['Validate all subventions', 'Validate some subventions'])
    any_answer_mask  = df['decision'].notna()
    won_mask         = df['subsidy_outcome'].eq('Won')
    in_progress_mask = df['decision'].isna()

    # Core/global KPIs you already had
    kpis = {}
    kpis['nombre_demandes_spontanees'] = int(spontaneous_mask.sum())
    kpis['montants_en_cours'] = float(df.loc[in_progress_mask, 'estimated_budget'].sum())
    kpis['nombre_demandes_validees'] = int(validated_mask.sum())
    kpis['montants_obtenus'] = float(df['amount_awarded'].sum())

    # Actions KPIs
    kpis['actions_realisees'] = int((action_plan['status'] == 'Completed').sum())
    kpis['actions_en_retard'] = int((action_plan['status'] == 'Late').sum())

    # ------- Your Plant & Efficiency KPIs
    # 1) Plant Requests
    kpis['plant_requests'] = int(planned_mask.sum())

    # 2) Success Rate (your formula: Won among validated)
    denom_validated = max(int(validated_mask.sum()), 1)
    kpis['success_rate_validated'] = float((won_mask & validated_mask).sum() / denom_validated * 100)

    # (Optional alt: Approved vs Submitted) - left here for completeness
    kpis['success_rate_submitted_alt'] = float(validated_mask.sum() / max(total_requests, 1) * 100)

    # 3) Impact on Competitiveness = Σ awarded / Σ estimated (percent)
    total_est = float(df['estimated_budget'].sum())
    total_awd = float(df['amount_awarded'].sum())
    kpis['impact_competitiveness'] = float((total_awd / total_est * 100) if total_est > 0 else 0.0)

    # 4) % Answers (positive answers among all submitted)
    kpis['percent_positive_answers'] = float(validated_mask.sum() / max(total_requests, 1) * 100)

    # (Optional alt: any answer, approved or rejected)
    kpis['percent_any_answer_alt'] = float(any_answer_mask.sum() / max(total_requests, 1) * 100)

    return kpis

def calculate_kpis_by_site(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    # Normalize
    df['estimated_budget'] = pd.to_numeric(df['estimated_budget'], errors='coerce').fillna(0)
    df['amount_awarded']  = pd.to_numeric(df['amount_awarded'],  errors='coerce').fillna(0)

    for col in ['request_type', 'decision', 'subsidy_outcome', 'site_location']:
        if col in df.columns:
            df[col] = _clean_text(df[col])
    if 'plant' in ap.columns:
        ap['plant'] = _clean_text(ap['plant'])

    # Aggregate across union of sites in both tables
    sites = sorted(set(df['site_location'].dropna().unique()) | set(ap['plant'].dropna().unique()))
    rows: List[Dict] = []

    for site in sites:
        site_req = df[df['site_location'] == site]
        site_ap  = ap[ap['plant'] == site]

        total = len(site_req)
        spontaneous_mask = site_req['request_type'].eq('Spontaneous requests')
        planned_mask     = site_req['request_type'].eq('Planned requests')  # Plant Requests
        validated_mask   = site_req['decision'].isin(['Validate all subventions', 'Validate some subventions'])
        won_mask         = site_req['subsidy_outcome'].eq('Won')
        in_progress_mask = site_req['decision'].isna()

        est_sum = float(site_req['estimated_budget'].sum())
        awd_sum = float(site_req['amount_awarded'].sum())

        # Actions
        completed = int((site_ap['status'] == 'Completed').sum())
        late      = int((site_ap['status'] == 'Late').sum())

        # KPIs per site (same names as global for consistency + your new ones)
        row = {
            'Site': site,
            'Demandes spontanées': int(spontaneous_mask.sum()),
            'Montants en cours (€)': float(site_req.loc[in_progress_mask, 'estimated_budget'].sum()),
            'Total demandes spontanées': int(spontaneous_mask.sum()),
            'Demandes validées': int(validated_mask.sum()),
            'Montants obtenus (€)': awd_sum,
            'Actions réalisées': completed,
            'Actions en retard': late,

            # Your added KPIs
            'Plant Requests': int(planned_mask.sum()),
            'Success Rate (validated)%': float((won_mask & validated_mask).sum() / max(int(validated_mask.sum()), 1) * 100),
            'Percent Positive Answers%': float(validated_mask.sum() / max(total, 1) * 100),
            'Impact on Competitiveness%': float((awd_sum / est_sum * 100) if est_sum > 0 else 0.0),
        }
        rows.append(row)

    return pd.DataFrame(rows)

def calculate_kpis_by_project(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    df = subsidy_requests.copy()
    ap = action_plan.copy()

    df['estimated_budget'] = pd.to_numeric(df['estimated_budget'], errors='coerce').fillna(0)
    df['amount_awarded']  = pd.to_numeric(df['amount_awarded'],  errors='coerce').fillna(0)

    for col in ['request_type', 'decision', 'subsidy_outcome', 'site_location']:
        if col in df.columns:
            df[col] = _clean_text(df[col])
    for col in ['project_title', 'plant', 'status']:
        if col in ap.columns:
            ap[col] = _clean_text(ap[col])

    projects = sorted(ap['project_title'].dropna().unique())
    rows: List[Dict] = []

    for project in projects:
        proj_actions = ap[ap['project_title'] == project]

        # Site for project = most frequent plant (if any)
        site = proj_actions['plant'].mode()[0] if not proj_actions['plant'].mode().empty else 'N/A'

        proj_reqs = df[df['site_location'] == site] if site != 'N/A' else df.iloc[0:0]

        total = len(proj_reqs)
        spontaneous_mask = proj_reqs['request_type'].eq('Spontaneous requests')
        planned_mask     = proj_reqs['request_type'].eq('Planned requests')  # Plant Requests
        validated_mask   = proj_reqs['decision'].isin(['Validate all subventions', 'Validate some subventions'])
        won_mask         = proj_reqs['subsidy_outcome'].eq('Won')
        in_progress_mask = proj_reqs['decision'].isna()

        est_sum = float(proj_reqs['estimated_budget'].sum())
        awd_sum = float(proj_reqs['amount_awarded'].sum())

        completed = int((proj_actions['status'] == 'Completed').sum())
        late      = int((proj_actions['status'] == 'Late').sum())

        row = {
            'Projet': project,
            'Site': site,
            'Demandes spontanées': int(spontaneous_mask.sum()),
            'Montants en cours (€)': float(proj_reqs.loc[in_progress_mask, 'estimated_budget'].sum()),
            'Total demandes spontanées': int(spontaneous_mask.sum()),
            'Demandes validées': int(validated_mask.sum()),
            'Montants obtenus (€)': awd_sum,
            'Actions réalisées': completed,
            'Actions en retard': late,

            # Your added KPIs
            'Plant Requests': int(planned_mask.sum()),
            'Success Rate (validated)%': float((won_mask & validated_mask).sum() / max(int(validated_mask.sum()), 1) * 100),
            'Percent Positive Answers%': float(validated_mask.sum() / max(total, 1) * 100),
            'Impact on Competitiveness%': float((awd_sum / est_sum * 100) if est_sum > 0 else 0.0),
        }
        rows.append(row)

    return pd.DataFrame(rows)

def get_detailed_data_for_visualization(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    viz_data = {}
    viz_data['requests_summary'] = subsidy_requests[[
        'name', 'site_location', 'request_type', 'decision',
        'estimated_budget', 'amount_awarded', 'date_creation'
    ]].copy()

    viz_data['actions_summary'] = action_plan[[
        'project_title', 'plant', 'status', 'action_type',
        'initiation_date', 'due_date', 'expected_gain'
    ]].copy()
    return viz_data

# ============================================================================
# API Routes
# ============================================================================
@app.route('/')
def home():
    return jsonify({
        "message": "Subsidy KPI API",
        "version": "1.1",
        "endpoints": {
            "/api/kpis/global": "GET - Global KPIs",
            "/api/kpis/by-site": "GET - KPIs by site",
            "/api/kpis/by-project": "GET - KPIs by project",
            "/api/kpis/all": "GET - All KPIs combined",
            "/api/data/requests": "GET - Subsidy requests data",
            "/api/data/actions": "GET - Action plan data",
            "/api/data/visualization": "GET - Data for visualization"
        }
    })

@app.route('/api/kpis/global', methods=['GET'])
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

@app.route('/api/kpis/by-site', methods=['GET'])
def get_kpis_by_site():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        kpis_df = calculate_kpis_by_site(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis_df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/kpis/by-project', methods=['GET'])
def get_kpis_by_project():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()

        kpis_df = calculate_kpis_by_project(subsidy_requests, action_plan)
        return jsonify({"success": True, "data": kpis_df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/kpis/all', methods=['GET'])
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
                "by_site": kpis_by_site.to_dict(orient='records'),
                "by_project": kpis_by_project.to_dict(orient='records')
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/data/requests', methods=['GET'])
def get_requests_data():
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        conn.close()

        data = json.loads(subsidy_requests.to_json(orient='records', date_format='iso'))
        return jsonify({"success": True, "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/data/actions', methods=['GET'])
def get_actions_data():
    try:
        conn = get_db_connection()
        action_plan = get_action_plan_data(conn)
        conn.close()

        data = json.loads(action_plan.to_json(orient='records', date_format='iso'))
        return jsonify({"success": True, "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/data/visualization', methods=['GET'])
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
                "requests_summary": json.loads(viz_data['requests_summary'].to_json(orient='records', date_format='iso')),
                "actions_summary": json.loads(viz_data['actions_summary'].to_json(orient='records', date_format='iso'))
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "error": "Internal server error"}), 500

# Run
if __name__ == '__main__':
    # host=0.0.0.0 for container/VM; change port if needed
    app.run(debug=True, host='0.0.0.0', port=5000)
