from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import pandas as pd
from typing import Dict, List
from datetime import date, datetime
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "Subsidy_DB",
    "user": "administrationSTS",
    "password": "St$@0987"
}

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Database connection error: {e}")

def get_subsidy_requests_data(connection) -> pd.DataFrame:
    """Retrieve relevant columns from subsidy_requests table for KPIs."""
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
    df = pd.read_sql_query(query, connection)
    return df

def get_action_plan_data(connection) -> pd.DataFrame:
    """Retrieve relevant columns from subsidy_action_plan table for KPIs."""
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
    df = pd.read_sql_query(query, connection)
    return df

def calculate_global_kpis(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict:
    """Calculate global KPIs (all sites combined)."""
    kpis = {}
    
    # Convert numeric columns to proper types
    subsidy_requests['estimated_budget'] = pd.to_numeric(subsidy_requests['estimated_budget'], errors='coerce').fillna(0)
    subsidy_requests['amount_awarded'] = pd.to_numeric(subsidy_requests['amount_awarded'], errors='coerce').fillna(0)
    
    # KPI 1: Nombre de demandes spontanées
    spontaneous_requests = subsidy_requests[subsidy_requests['request_type'] == 'Spontaneous requests']
    kpis['nombre_demandes_spontanees'] = len(spontaneous_requests)
    
    # KPI 2: Montants actuellement en cours de traitement
    in_progress = subsidy_requests[subsidy_requests['decision'].isna()]
    kpis['montants_en_cours'] = float(in_progress['estimated_budget'].sum())
    
    # KPI 3: Nombre total de demandes spontanées effectuées
    kpis['total_demandes_spontanees'] = len(spontaneous_requests)
    
    # KPI 4: Nombre de demandes validées par les usines
    validated = subsidy_requests[
        subsidy_requests['decision'].isin(['Validate all subventions', 'Validate some subventions'])
    ]
    kpis['nombre_demandes_validees'] = len(validated)
    
    # KPI 5: Montants obtenus
    kpis['montants_obtenus'] = float(subsidy_requests['amount_awarded'].sum())
    
    # KPI 6: Actions réalisées et actions en retard
    completed_actions = action_plan[action_plan['status'] == 'Completed']
    kpis['actions_realisees'] = len(completed_actions)
    
    late_actions = action_plan[action_plan['status'] == 'Late']
    kpis['actions_en_retard'] = len(late_actions)
    
    return kpis

def calculate_kpis_by_site(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    """Calculate KPIs grouped by site location."""
    # Convert numeric columns
    subsidy_requests['estimated_budget'] = pd.to_numeric(subsidy_requests['estimated_budget'], errors='coerce').fillna(0)
    subsidy_requests['amount_awarded'] = pd.to_numeric(subsidy_requests['amount_awarded'], errors='coerce').fillna(0)
    
    # Get unique sites from both tables
    sites_requests = set(subsidy_requests['site_location'].dropna().unique())
    sites_actions = set(action_plan['plant'].dropna().unique())
    all_sites = sorted(sites_requests.union(sites_actions))
    
    kpis_by_site = []
    
    for site in all_sites:
        site_requests = subsidy_requests[subsidy_requests['site_location'] == site]
        site_actions = action_plan[action_plan['plant'] == site]
        
        spontaneous = site_requests[site_requests['request_type'] == 'Spontaneous requests']
        in_progress = site_requests[site_requests['decision'].isna()]
        validated = site_requests[site_requests['decision'].isin(['Validate all subventions', 'Validate some subventions'])]
        completed = site_actions[site_actions['status'] == 'Completed']
        late = site_actions[site_actions['status'] == 'Late']
        
        kpis_by_site.append({
            'Site': site,
            'Demandes spontanées': len(spontaneous),
            'Montants en cours (€)': float(in_progress['estimated_budget'].sum()),
            'Total demandes spontanées': len(spontaneous),
            'Demandes validées': len(validated),
            'Montants obtenus (€)': float(site_requests['amount_awarded'].sum()),
            'Actions réalisées': len(completed),
            'Actions en retard': len(late)
        })
    
    return pd.DataFrame(kpis_by_site)

def calculate_kpis_by_project(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> pd.DataFrame:
    """Calculate KPIs grouped by project title."""
    # Convert numeric columns
    subsidy_requests['estimated_budget'] = pd.to_numeric(subsidy_requests['estimated_budget'], errors='coerce').fillna(0)
    subsidy_requests['amount_awarded'] = pd.to_numeric(subsidy_requests['amount_awarded'], errors='coerce').fillna(0)
    
    # Get unique projects from action plan
    projects = action_plan['project_title'].dropna().unique()
    
    kpis_by_project = []
    
    for project in sorted(projects):
        project_actions = action_plan[action_plan['project_title'] == project]
        
        # Get the plant/site for this project (take the most common one)
        site = project_actions['plant'].mode()[0] if len(project_actions['plant'].mode()) > 0 else 'N/A'
        
        # Find related subsidy requests by site
        project_requests = subsidy_requests[subsidy_requests['site_location'] == site]
        
        spontaneous = project_requests[project_requests['request_type'] == 'Spontaneous requests']
        in_progress = project_requests[project_requests['decision'].isna()]
        validated = project_requests[project_requests['decision'].isin(['Validate all subventions', 'Validate some subventions'])]
        completed = project_actions[project_actions['status'] == 'Completed']
        late = project_actions[project_actions['status'] == 'Late']
        
        kpis_by_project.append({
            'Projet': project,
            'Site': site,
            'Demandes spontanées': len(spontaneous),
            'Montants en cours (€)': float(in_progress['estimated_budget'].sum()),
            'Total demandes spontanées': len(spontaneous),
            'Demandes validées': len(validated),
            'Montants obtenus (€)': float(project_requests['amount_awarded'].sum()),
            'Actions réalisées': len(completed),
            'Actions en retard': len(late)
        })
    
    return pd.DataFrame(kpis_by_project)

def get_detailed_data_for_visualization(subsidy_requests: pd.DataFrame, action_plan: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Prepare detailed datasets for visualization with selected columns."""
    viz_data = {}
    
    # Subsidy requests with key columns for visualization
    viz_data['requests_summary'] = subsidy_requests[[
        'name', 'site_location', 'request_type', 'decision', 
        'estimated_budget', 'amount_awarded', 'date_creation'
    ]].copy()
    
    # Action plan with key columns for visualization
    viz_data['actions_summary'] = action_plan[[
        'project_title', 'plant', 'status', 'action_type',
        'initiation_date', 'due_date', 'expected_gain'
    ]].copy()
    
    return viz_data

# API Routes

@app.route('/')
def home():
    """Home endpoint with API documentation."""
    return jsonify({
        "message": "Subsidy KPI API",
        "version": "1.0",
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
    """Get global KPIs."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        
        kpis = calculate_global_kpis(subsidy_requests, action_plan)
        return jsonify({
            "success": True,
            "data": kpis
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/kpis/by-site', methods=['GET'])
def get_kpis_by_site():
    """Get KPIs grouped by site."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        
        kpis_df = calculate_kpis_by_site(subsidy_requests, action_plan)
        return jsonify({
            "success": True,
            "data": kpis_df.to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/kpis/by-project', methods=['GET'])
def get_kpis_by_project():
    """Get KPIs grouped by project."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        action_plan = get_action_plan_data(conn)
        conn.close()
        
        kpis_df = calculate_kpis_by_project(subsidy_requests, action_plan)
        return jsonify({
            "success": True,
            "data": kpis_df.to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/kpis/all', methods=['GET'])
def get_all_kpis():
    """Get all KPIs (global, by site, and by project)."""
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
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/data/requests', methods=['GET'])
def get_requests_data():
    """Get subsidy requests data."""
    try:
        conn = get_db_connection()
        subsidy_requests = get_subsidy_requests_data(conn)
        conn.close()
        
        # Convert DataFrame to dict with proper date handling
        data = json.loads(subsidy_requests.to_json(orient='records', date_format='iso'))
        
        return jsonify({
            "success": True,
            "count": len(data),
            "data": data
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/data/actions', methods=['GET'])
def get_actions_data():
    """Get action plan data."""
    try:
        conn = get_db_connection()
        action_plan = get_action_plan_data(conn)
        conn.close()
        
        # Convert DataFrame to dict with proper date handling
        data = json.loads(action_plan.to_json(orient='records', date_format='iso'))
        
        return jsonify({
            "success": True,
            "count": len(data),
            "data": data
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/data/visualization', methods=['GET'])
def get_visualization_data():
    """Get data prepared for visualization."""
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
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "Endpoint not found"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "success": False,
        "error": "Internal server error"
    }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)