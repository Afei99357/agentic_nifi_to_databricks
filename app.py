import os
import logging
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.exceptions import HTTPException
import io

from utils.databricks_client import DatabricksClientWrapper
from services.migration_service import MigrationService
from services.flow_service import FlowService
from services.notebook_service import NotebookService
from services.job_deployment import JobDeploymentService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Initialize Flask app
flask_app = Flask(__name__)
flask_app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

# Initialize services
try:
    logging.info("Starting service initialization...")
    logging.info("Initializing DatabricksClientWrapper...")
    databricks_client = DatabricksClientWrapper()
    logging.info("✓ DatabricksClientWrapper initialized")

    logging.info("Initializing MigrationService (new schema)...")
    migration_service = MigrationService()
    logging.info("✓ MigrationService initialized")

    logging.info("Initializing FlowService...")
    flow_service = FlowService(migration_service)
    logging.info("✓ FlowService initialized")

    logging.info("Initializing NotebookService...")
    notebook_service = NotebookService(databricks_client)
    logging.info("✓ NotebookService initialized")

    logging.info("Initializing JobDeploymentService...")
    job_deployment_service = JobDeploymentService(databricks_client)
    logging.info("✓ JobDeploymentService initialized")

    logging.info("All services initialized successfully")
except ValueError as e:
    # Configuration error (missing env vars)
    logging.error(f"Configuration error: {e}")
    logging.error("For Databricks Apps: Ensure DATABRICKS_HTTP_PATH is configured in app.yaml")
    logging.error("For local dev: Set DATABRICKS_TOKEN, DATABRICKS_HOST, and DATABRICKS_HTTP_PATH")
    databricks_client = None
    migration_service = None
    flow_service = None
    notebook_service = None
    job_deployment_service = None
except Exception as e:
    logging.error(f"Failed to initialize services: {e}")
    import traceback
    logging.error(traceback.format_exc())
    databricks_client = None
    migration_service = None
    flow_service = None
    notebook_service = None
    job_deployment_service = None


# ===== ROUTES =====

@flask_app.route('/')
def dashboard():
    """Main dashboard showing all flows."""
    return render_template('dashboard.html')


# ===== API ROUTES =====

# Flow Management Endpoints

@flask_app.route('/api/flows', methods=['GET'])
def api_list_flows():
    """Get all flows from migration_flows table."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        flows = migration_service.list_all_flows()
        flow_dicts = [flow.to_dict() for flow in flows]
        logging.info(f"Successfully fetched {len(flow_dicts)} flows from migration_flows table")
        return jsonify({
            'success': True,
            'flows': flow_dicts,
            'count': len(flow_dicts)
        })
    except Exception as e:
        logging.error(f"Error fetching flows: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/debug/connection', methods=['GET'])
def api_debug_connection():
    """Debug endpoint to check MigrationService connection and configuration."""
    if migration_service is None:
        return jsonify({
            'success': False,
            'error': 'MigrationService not initialized',
            'services_initialized': False
        }), 503

    try:
        # Test connection and query
        flows = migration_service.list_all_flows()

        return jsonify({
            'success': True,
            'services_initialized': True,
            'connection_info': {
                'schema': migration_service.schema,
                'flows_table': migration_service.flows_table,
                'warehouse_id': getattr(migration_service.cfg, 'warehouse_id', 'NOT_SET'),
                'connection_open': migration_service._connection is not None and migration_service._connection.open if migration_service._connection else False
            },
            'query_result': {
                'flows_count': len(flows),
                'sample_flows': [{'flow_id': f.flow_id, 'flow_name': f.flow_name, 'status': f.migration_status} for f in flows[:5]]
            }
        })
    except Exception as e:
        logging.error(f"Debug connection test failed: {e}")
        import traceback
        error_traceback = traceback.format_exc()
        logging.error(error_traceback)
        return jsonify({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__,
            'traceback': error_traceback
        }), 500


@flask_app.route('/api/flows/<flow_identifier>', methods=['GET'])
def api_get_flow(flow_identifier: str):
    """
    Get details for a specific flow with latest status.

    Args:
        flow_identifier: Either flow_id (UUID) or flow_name (for backward compat)
    """
    if flow_service is None or migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Try to get by flow_id first, then by flow_name
        flow = migration_service.get_flow(flow_identifier)
        if not flow:
            flow = migration_service.get_flow_by_name(flow_identifier)

        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        # Poll latest status from Databricks if running
        flow_dict = flow_service.poll_flow_status(flow.flow_id)

        return jsonify({
            'success': True,
            'flow': flow_dict
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/start', methods=['POST'])
def api_start_conversion(flow_identifier: str):
    """
    Kick off conversion job for a flow.

    Args:
        flow_identifier: Either flow_id (UUID) or flow_name (for backward compat)
    """
    if flow_service is None or migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Resolve to flow_id
        flow = migration_service.get_flow(flow_identifier)
        if not flow:
            flow = migration_service.get_flow_by_name(flow_identifier)

        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        result = flow_service.start_conversion(flow.flow_id)

        if not result['success']:
            return jsonify(result), 400

        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/bulk-start', methods=['POST'])
def api_bulk_start_conversions():
    """Start conversions for multiple flows."""
    if flow_service is None or migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        data = request.json
        flow_identifiers = data.get('flow_names', []) or data.get('flow_ids', [])  # Support both for backward compat

        if not flow_identifiers:
            return jsonify({'success': False, 'error': 'flow_names or flow_ids required'}), 400

        # Resolve all identifiers to flow_ids
        flow_ids = []
        for identifier in flow_identifiers:
            flow = migration_service.get_flow(identifier)
            if not flow:
                flow = migration_service.get_flow_by_name(identifier)
            if flow:
                flow_ids.append(flow.flow_id)

        result = flow_service.start_bulk_conversions(flow_ids)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/stop', methods=['POST'])
def api_stop_conversion(flow_identifier: str):
    """Cancel a running conversion."""
    if flow_service is None or migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Resolve to flow_id
        flow = migration_service.get_flow(flow_identifier)
        if not flow:
            flow = migration_service.get_flow_by_name(flow_identifier)

        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        result = flow_service.stop_conversion(flow.flow_id)

        if not result['success']:
            return jsonify(result), 400

        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/notebooks', methods=['GET'])
def api_get_notebooks(flow_identifier: str):
    """Get list of generated notebooks for a flow."""
    if flow_service is None or migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Resolve to flow_id
        flow = migration_service.get_flow(flow_identifier)
        if not flow:
            flow = migration_service.get_flow_by_name(flow_identifier)

        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        notebooks = flow_service.get_flow_notebooks(flow.flow_id)

        return jsonify({
            'success': True,
            'notebooks': notebooks
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/history', methods=['GET'])
def api_get_flow_history(flow_identifier: str):
    """Get conversion history for a flow (migration runs)."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Resolve to flow_id
        flow = migration_service.get_flow(flow_identifier)
        if not flow:
            flow = migration_service.get_flow_by_name(flow_identifier)

        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        limit = request.args.get('limit', default=20, type=int)
        runs = migration_service.get_flow_runs(flow.flow_id, limit=limit)

        return jsonify({
            'success': True,
            'flow_id': flow.flow_id,
            'flow_name': flow.flow_name,
            'history': [run.to_dict() for run in runs],
            'count': len(runs)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# New granular data routes for Cori's architecture

@flask_app.route('/api/flows/<flow_identifier>/runs/<run_id>/iterations', methods=['GET'])
def api_get_run_iterations(flow_identifier: str, run_id: str):
    """Get all iterations for a run (step-by-step progress)."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized.'}), 503

    try:
        iterations = migration_service.get_run_iterations(run_id)
        return jsonify({
            'success': True,
            'run_id': run_id,
            'iterations': [it.to_dict() for it in iterations],
            'count': len(iterations)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/runs/<run_id>/patches', methods=['GET'])
def api_get_run_patches(flow_identifier: str, run_id: str):
    """Get all code patches for a run."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized.'}), 503

    try:
        patches = migration_service.get_run_patches(run_id)
        return jsonify({
            'success': True,
            'run_id': run_id,
            'patches': [p.to_dict() for p in patches],
            'count': len(patches)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/runs/<run_id>/sast', methods=['GET'])
def api_get_run_sast_results(flow_identifier: str, run_id: str):
    """Get SAST scan results for a run."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized.'}), 503

    try:
        sast_results = migration_service.get_run_sast_results(run_id)
        return jsonify({
            'success': True,
            'run_id': run_id,
            'sast_results': [sr.to_dict() for sr in sast_results],
            'count': len(sast_results)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/runs/<run_id>/exec_results', methods=['GET'])
def api_get_run_exec_results(flow_identifier: str, run_id: str):
    """Get job execution results for a run."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized.'}), 503

    try:
        exec_results = migration_service.get_run_exec_results(run_id)
        return jsonify({
            'success': True,
            'run_id': run_id,
            'exec_results': [er.to_dict() for er in exec_results],
            'count': len(exec_results)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_identifier>/runs/<run_id>/human_requests', methods=['GET'])
def api_get_run_human_requests(flow_identifier: str, run_id: str):
    """Get human intervention requests for a run."""
    if migration_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized.'}), 503

    try:
        human_requests = migration_service.get_run_human_requests(run_id)
        return jsonify({
            'success': True,
            'run_id': run_id,
            'human_requests': [hr.to_dict() for hr in human_requests],
            'count': len(human_requests)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>/download', methods=['GET'])
def api_download_notebooks(flow_name: str):
    """Download generated notebooks as ZIP."""
    if delta_service is None or notebook_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        flow = delta_service.get_flow(flow_name)

        if not flow or not flow.generated_notebooks:
            return jsonify({'success': False, 'error': 'No notebooks found'}), 404

        # Create notebook objects for download
        from models.nifi_flow import Notebook
        notebooks = []
        for notebook_path in flow.generated_notebooks:
            notebook_name = os.path.basename(notebook_path)
            nb = Notebook(
                notebook_id=notebook_name,
                notebook_name=notebook_name,
                flow_name=flow.flow_name,
                content=f"# Notebook: {notebook_name}\n# Flow: {flow.flow_name}\n",
                language="python",
                volume_path=notebook_path
            )
            notebooks.append(nb)

        zip_content = notebook_service.download_all_notebooks(notebooks)
        return send_file(
            io.BytesIO(zip_content),
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'{flow.flow_name}_notebooks.zip'
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>/deploy-and-run', methods=['POST'])
def api_deploy_and_run(flow_name):
    """Deploy generated notebooks as a Databricks job and run it."""
    if delta_service is None or notebook_service is None or job_deployment_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Get flow from Delta table
        flow = delta_service.get_flow(flow_name)
        if not flow:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        if not flow.generated_notebooks:
            return jsonify({'success': False, 'error': 'No notebooks generated yet'}), 400

        data = request.json
        job_name = data.get('job_name', f'nifi_execution_{flow.flow_name}')
        run_immediately = data.get('run_immediately', True)

        # Create notebook objects with volume paths
        from models.nifi_flow import Notebook
        notebooks = []
        output_path = f"/Volumes/main/default/nifi_notebooks/{flow.flow_name}"

        for notebook_path in flow.generated_notebooks:
            notebook_name = os.path.basename(notebook_path)
            nb = Notebook(
                notebook_id=notebook_name,
                notebook_name=notebook_name,
                flow_name=flow.flow_name,
                content=f"# Notebook: {notebook_name}\n",
                language="python",
                volume_path=notebook_path
            )
            notebooks.append(nb)

        # Save notebooks to volume (if needed)
        notebook_service.save_notebooks_to_volume(notebooks, output_path)

        # Create and optionally run job
        if run_immediately:
            db_job_id, run_id = job_deployment_service.create_and_run_job(job_name, notebooks)
            return jsonify({
                'success': True,
                'job_id': db_job_id,
                'run_id': run_id,
                'message': 'Job created and started successfully'
            })
        else:
            db_job_id = job_deployment_service.create_parallel_job(job_name, notebooks)
            return jsonify({
                'success': True,
                'job_id': db_job_id,
                'message': 'Job created successfully'
            })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/run/<run_id>/status', methods=['GET'])
def api_run_status(run_id):
    """Get job run status (for polling)."""
    if job_deployment_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        status = job_deployment_service.get_job_run_status(run_id)
        return jsonify({
            'success': True,
            'status': status.to_dict()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/run/<run_id>/tasks', methods=['GET'])
def api_run_tasks(run_id):
    """Get task statuses for a job run."""
    if job_deployment_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        tasks = job_deployment_service.get_task_statuses(run_id)
        return jsonify({
            'success': True,
            'tasks': [task.to_dict() for task in tasks]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/run/<run_id>/task/<task_key>/logs', methods=['GET'])
def api_task_logs(run_id, task_key):
    """Get logs for a specific task."""
    if job_deployment_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        logs = job_deployment_service.get_task_logs(run_id, task_key)
        return jsonify({
            'success': True,
            'logs': logs
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/run/<run_id>/cancel', methods=['POST'])
def api_cancel_run(run_id):
    """Cancel a running job."""
    if job_deployment_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        success = job_deployment_service.cancel_job_run(run_id)
        return jsonify({
            'success': success,
            'message': 'Job cancelled successfully' if success else 'Failed to cancel job'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ===== ERROR HANDLERS =====

@flask_app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Handle HTTP exceptions."""
    return jsonify({'success': False, 'error': str(e)}), e.code


@flask_app.errorhandler(Exception)
def handle_exception(e):
    """Handle all other exceptions."""
    logging.error(f"Unhandled exception: {e}", exc_info=True)
    return jsonify({'success': False, 'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Use PORT environment variable for Databricks Apps, fallback to 8080 for local dev
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(debug=True, host='0.0.0.0', port=port)
