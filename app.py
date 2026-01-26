import os
import logging
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.exceptions import HTTPException
import io

from utils.databricks_client import DatabricksClientWrapper
from services.delta_service import DeltaService
from services.flow_service import FlowService
from services.notebook_service import NotebookService
from services.job_deployment import JobDeploymentService

# Configure logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Initialize Flask app
flask_app = Flask(__name__)
flask_app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

# Initialize services
try:
    databricks_client = DatabricksClientWrapper()
    delta_service = DeltaService()
    flow_service = FlowService(delta_service)
    notebook_service = NotebookService(databricks_client)
    job_deployment_service = JobDeploymentService(databricks_client)
    logging.info("All services initialized successfully")
except ValueError as e:
    # Configuration error (missing env vars)
    logging.error(f"Configuration error: {e}")
    logging.error("Required environment variables: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN")
    logging.error("Please configure SQL Warehouse connection details in app settings")
    databricks_client = None
    delta_service = None
    flow_service = None
    notebook_service = None
    job_deployment_service = None
except Exception as e:
    logging.error(f"Failed to initialize services: {e}")
    import traceback
    logging.error(traceback.format_exc())
    databricks_client = None
    delta_service = None
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
    """Get all flows from Delta table."""
    if delta_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        flows = delta_service.list_all_flows()
        return jsonify({
            'success': True,
            'flows': [flow.to_dict() for flow in flows]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>', methods=['GET'])
def api_get_flow(flow_name: str):
    """Get details for a specific flow with latest status."""
    if flow_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        # Poll latest status from Databricks if converting
        flow_dict = flow_service.poll_flow_status(flow_name)

        if not flow_dict:
            return jsonify({'success': False, 'error': 'Flow not found'}), 404

        return jsonify({
            'success': True,
            'flow': flow_dict
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>/start', methods=['POST'])
def api_start_conversion(flow_name: str):
    """Kick off conversion job for a flow."""
    if flow_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        result = flow_service.start_conversion(flow_name)

        if not result['success']:
            return jsonify(result), 400

        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/bulk-start', methods=['POST'])
def api_bulk_start_conversions():
    """Start conversions for multiple flows."""
    if flow_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        data = request.json
        flow_names = data.get('flow_names', [])

        if not flow_names:
            return jsonify({'success': False, 'error': 'flow_names required'}), 400

        result = flow_service.start_bulk_conversions(flow_names)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>/stop', methods=['POST'])
def api_stop_conversion(flow_name: str):
    """Cancel a running conversion."""
    if flow_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        result = flow_service.stop_conversion(flow_name)

        if not result['success']:
            return jsonify(result), 400

        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>/notebooks', methods=['GET'])
def api_get_notebooks(flow_name: str):
    """Get list of generated notebooks for a flow."""
    if flow_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        notebooks = flow_service.get_flow_notebooks(flow_name)

        return jsonify({
            'success': True,
            'notebooks': notebooks
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/flows/<flow_name>/history', methods=['GET'])
def api_get_flow_history(flow_name: str):
    """Get conversion history for a flow."""
    if delta_service is None:
        return jsonify({'success': False, 'error': 'Service not initialized. Check if app is running in Databricks environment.'}), 503

    try:
        limit = request.args.get('limit', default=20, type=int)
        history = delta_service.get_flow_history(flow_name, limit=limit)

        return jsonify({
            'success': True,
            'flow_name': flow_name,
            'history': history,
            'count': len(history)
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
