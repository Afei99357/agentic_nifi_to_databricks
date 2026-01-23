import os
import logging
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.exceptions import HTTPException
import io

from utils.databricks_client import DatabricksClientWrapper
from services.unity_catalog import UnityCatalogService
from services.agent_service import AgentService
from services.notebook_service import NotebookService
from services.nifi_parser import NiFiParser
from services.job_deployment import JobDeploymentService
from models.conversion_job import JobStatus

# Configure logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Initialize Flask app
flask_app = Flask(__name__)
flask_app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

# Initialize services
try:
    databricks_client = DatabricksClientWrapper()
    unity_catalog_service = UnityCatalogService(databricks_client)
    agent_service = AgentService()
    notebook_service = NotebookService(databricks_client)
    job_deployment_service = JobDeploymentService(databricks_client)
except Exception as e:
    logging.error(f"Failed to initialize services: {e}")
    databricks_client = None
    unity_catalog_service = None
    agent_service = None
    notebook_service = None
    job_deployment_service = None


# ===== ROUTES =====

@flask_app.route('/')
def index():
    """Landing page."""
    return render_template('index.html')


@flask_app.route('/step1')
def step1():
    """Step 1: File selection UI."""
    return render_template('step1_select.html')


@flask_app.route('/step2/<job_id>')
def step2(job_id):
    """Step 2: Review conversion results."""
    return render_template('step2_review.html', job_id=job_id)


@flask_app.route('/step3/<job_id>')
def step3(job_id):
    """Step 3: Deployment configuration."""
    return render_template('step3_deploy.html', job_id=job_id)


@flask_app.route('/step3b/<run_id>')
def step3b(run_id):
    """Step 3b: Execution monitoring."""
    return render_template('step3b_monitor.html', run_id=run_id)


# ===== API ROUTES =====

@flask_app.route('/api/volumes', methods=['GET'])
def api_list_volumes():
    """List Unity Catalog volumes."""
    try:
        catalog = request.args.get('catalog', 'main')
        schema = request.args.get('schema', 'default')

        volumes = unity_catalog_service.list_volumes(catalog, schema)
        return jsonify({
            'success': True,
            'volumes': [v.to_dict() for v in volumes]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/files', methods=['GET'])
def api_list_files():
    """List files in a volume path."""
    try:
        volume_path = request.args.get('path')
        if not volume_path:
            return jsonify({'success': False, 'error': 'Path parameter required'}), 400

        filter_xml = request.args.get('filter_xml', 'true').lower() == 'true'
        files = unity_catalog_service.list_files(volume_path, filter_xml=filter_xml)

        return jsonify({
            'success': True,
            'files': [f.to_dict() for f in files]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/file/preview', methods=['POST'])
def api_preview_file():
    """Preview a NiFi XML file."""
    try:
        data = request.json
        file_path = data.get('file_path')

        if not file_path:
            return jsonify({'success': False, 'error': 'File path required'}), 400

        xml_content = unity_catalog_service.read_nifi_xml(file_path)
        validation = unity_catalog_service.validate_xml_structure(xml_content)
        preview = NiFiParser.get_xml_preview(xml_content)

        return jsonify({
            'success': True,
            'preview': preview,
            'is_valid': validation.is_valid,
            'validation_message': validation.message
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/convert', methods=['POST'])
def api_start_conversion():
    """Start conversion of NiFi XML to notebooks."""
    try:
        data = request.json
        file_path = data.get('file_path')

        if not file_path:
            return jsonify({'success': False, 'error': 'File path required'}), 400

        # Read XML content
        xml_content = unity_catalog_service.read_nifi_xml(file_path)

        # Validate XML
        validation = unity_catalog_service.validate_xml_structure(xml_content)
        if not validation.is_valid:
            return jsonify({
                'success': False,
                'error': f'Invalid NiFi XML: {validation.message}'
            }), 400

        # Get user ID
        user_id = databricks_client.get_current_user()

        # Start conversion
        source_file = os.path.basename(file_path)
        source_volume_path = os.path.dirname(file_path)

        job = agent_service.start_conversion(
            nifi_xml=xml_content,
            source_file=source_file,
            source_volume_path=source_volume_path,
            user_id=user_id
        )

        return jsonify({
            'success': True,
            'job': job.to_dict()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/job/<job_id>/status', methods=['GET'])
def api_job_status(job_id):
    """Get conversion job status (for polling)."""
    try:
        job = agent_service.get_conversion_status(job_id)
        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        return jsonify({
            'success': True,
            'job': job.to_dict()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/job/<job_id>/notebooks', methods=['GET'])
def api_job_notebooks(job_id):
    """Get generated notebooks for a job."""
    try:
        job = agent_service.get_conversion_status(job_id)
        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        if job.status != JobStatus.COMPLETED:
            return jsonify({
                'success': False,
                'error': f'Job not completed. Status: {job.status.value}'
            }), 400

        # Generate notebooks with preview
        notebooks = []
        for notebook_name in job.generated_notebooks:
            # Create a simple notebook object for preview
            from models.nifi_flow import Notebook
            nb = Notebook(
                notebook_id=notebook_name,
                notebook_name=notebook_name,
                flow_id=notebook_name.replace('.py', ''),
                content=f"# Notebook: {notebook_name}\n# Generated from {job.source_file}\n",
                language="python"
            )
            preview = notebook_service.get_notebook_preview(nb)
            notebooks.append({
                'notebook_id': nb.notebook_id,
                'notebook_name': nb.notebook_name,
                'flow_id': nb.flow_id,
                'content': nb.content,
                'preview_html': preview.highlighted_html
            })

        return jsonify({
            'success': True,
            'notebooks': notebooks
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/job/<job_id>/download/<notebook_name>', methods=['GET'])
def api_download_notebook(job_id, notebook_name):
    """Download a specific notebook."""
    try:
        job = agent_service.get_conversion_status(job_id)
        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        if notebook_name not in job.generated_notebooks:
            return jsonify({'success': False, 'error': 'Notebook not found'}), 404

        # Create notebook object
        from models.nifi_flow import Notebook
        nb = Notebook(
            notebook_id=notebook_name,
            notebook_name=notebook_name,
            flow_id=notebook_name.replace('.py', ''),
            content=f"# Notebook: {notebook_name}\n",
            language="python"
        )

        content = notebook_service.download_notebook(nb)
        return send_file(
            io.BytesIO(content),
            mimetype='text/x-python',
            as_attachment=True,
            download_name=notebook_name
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/job/<job_id>/download-all', methods=['GET'])
def api_download_all_notebooks(job_id):
    """Download all notebooks as a ZIP file."""
    try:
        job = agent_service.get_conversion_status(job_id)
        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        # Create notebook objects
        from models.nifi_flow import Notebook
        notebooks = []
        for notebook_name in job.generated_notebooks:
            nb = Notebook(
                notebook_id=notebook_name,
                notebook_name=notebook_name,
                flow_id=notebook_name.replace('.py', ''),
                content=f"# Notebook: {notebook_name}\n",
                language="python"
            )
            notebooks.append(nb)

        zip_content = notebook_service.download_all_notebooks(notebooks)
        return send_file(
            io.BytesIO(zip_content),
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'notebooks_{job_id}.zip'
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@flask_app.route('/api/job/<job_id>/deploy-and-run', methods=['POST'])
def api_deploy_and_run(job_id):
    """Deploy notebooks as a Databricks job and run it."""
    try:
        job = agent_service.get_conversion_status(job_id)
        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        data = request.json
        job_name = data.get('job_name', f'nifi_conversion_{job.source_file}')
        run_immediately = data.get('run_immediately', True)

        # Create notebook objects with volume paths
        from models.nifi_flow import Notebook
        notebooks = []
        output_path = f"/Volumes/main/default/nifi_notebooks/{job.source_file}"

        for notebook_name in job.generated_notebooks:
            nb = Notebook(
                notebook_id=notebook_name,
                notebook_name=notebook_name,
                flow_id=notebook_name.replace('.py', ''),
                content=f"# Notebook: {notebook_name}\n",
                language="python",
                volume_path=f"{output_path}/{notebook_name}"
            )
            notebooks.append(nb)

        # Save notebooks to volume
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
