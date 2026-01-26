"""
Business logic for NiFi flow conversion operations.
"""

import os
from typing import List, Optional
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from services.delta_service import DeltaService


class FlowService:
    """Handles flow conversion lifecycle and Databricks job orchestration."""

    def __init__(self, delta_service: DeltaService = None):
        """
        Initialize flow service.

        Args:
            delta_service: DeltaService instance. If None, will be created.
        """
        self.delta_service = delta_service or DeltaService()
        self.conversion_job_id = os.getenv("NIFI_CONVERSION_JOB_ID")

        if not self.conversion_job_id:
            raise ValueError("NIFI_CONVERSION_JOB_ID environment variable not set")

    def start_conversion(self, flow_id: str) -> dict:
        """
        Kick off Databricks job for a single flow.

        Args:
            flow_id: Flow identifier

        Returns:
            dict with keys: success (bool), run_id (str), flow_id (str), error (str)
        """
        # Get flow details
        flow = self.delta_service.get_flow(flow_id)
        if not flow:
            return {'success': False, 'error': 'Flow not found'}

        if flow.status == 'CONVERTING':
            return {'success': False, 'error': 'Conversion already running'}

        try:
            # Kick off Databricks job
            config = Config(retry_timeout_seconds=300, max_retries=3)
            w = WorkspaceClient(config=config)

            # Prepare job parameters
            params = {
                "flow_id": flow_id,
                "nifi_xml_path": flow.nifi_xml_path or ""
            }

            run = w.jobs.run_now(
                job_id=int(self.conversion_job_id),
                job_parameters=params
            )

            # Update Delta table
            self.delta_service.update_flow_status(flow_id, {
                'status': 'CONVERTING',
                'databricks_run_id': str(run.run_id),
                'conversion_started_at': datetime.now().isoformat(),
                'status_message': 'Conversion job started',
                'error_message': None
            })

            return {
                'success': True,
                'run_id': str(run.run_id),
                'flow_id': flow_id
            }

        except Exception as e:
            # Update status to needs attention
            self.delta_service.update_flow_status(flow_id, {
                'status': 'NEEDS_ATTENTION',
                'error_message': str(e),
                'status_message': 'Failed to start conversion'
            })
            return {'success': False, 'error': str(e)}

    def start_bulk_conversions(self, flow_ids: List[str]) -> dict:
        """
        Start conversions for multiple flows.

        Args:
            flow_ids: List of flow identifiers

        Returns:
            dict with results for each flow
        """
        results = []
        for flow_id in flow_ids:
            result = self.start_conversion(flow_id)
            results.append(result)

        return {
            'success': True,
            'results': results,
            'started': len([r for r in results if r['success']]),
            'failed': len([r for r in results if not r['success']])
        }

    def poll_flow_status(self, flow_id: str) -> dict:
        """
        Poll Databricks for current status and update Delta table.

        Args:
            flow_id: Flow identifier

        Returns:
            Updated flow as dictionary
        """
        flow = self.delta_service.get_flow(flow_id)
        if not flow:
            return {}

        # If no run ID or already terminal, return cached
        if not flow.databricks_run_id or flow.status in ['DONE', 'NEEDS_ATTENTION']:
            return flow.to_dict()

        # Poll Databricks
        w = WorkspaceClient()
        try:
            run = w.jobs.get_run(run_id=int(flow.databricks_run_id))
            db_state = run.state.life_cycle_state.value

            updates = {}

            if db_state in ['PENDING', 'RUNNING', 'TERMINATING']:
                # Estimate progress based on elapsed time
                if run.start_time:
                    elapsed_min = (datetime.now().timestamp() * 1000 - run.start_time) / 1000 / 60
                    # Rough estimate: 10% per minute, cap at 90%
                    updates['progress_percentage'] = min(int(elapsed_min * 10), 90)
                updates['status_message'] = f'Job {db_state.lower()}'

            elif db_state == 'TERMINATED':
                result_state = run.state.result_state.value if run.state.result_state else 'UNKNOWN'

                if result_state == 'SUCCESS':
                    updates['status'] = 'DONE'
                    updates['progress_percentage'] = 100
                    updates['conversion_completed_at'] = datetime.now().isoformat()
                    updates['status_message'] = 'Conversion completed'
                    # TODO: Retrieve generated notebooks from job output

                elif result_state == 'FAILED':
                    updates['status'] = 'NEEDS_ATTENTION'
                    updates['error_message'] = run.state.state_message or 'Job failed'
                    updates['status_message'] = 'Conversion failed'

                else:
                    # CANCELED or other terminal states
                    updates['status'] = 'NEEDS_ATTENTION'
                    updates['status_message'] = f'Job {result_state.lower()}'

            # Apply updates if any
            if updates:
                self.delta_service.update_flow_status(flow_id, updates)

            # Return updated flow
            return self.delta_service.get_flow(flow_id).to_dict()

        except Exception as e:
            self.delta_service.update_flow_status(flow_id, {
                'status': 'NEEDS_ATTENTION',
                'error_message': f'Polling failed: {str(e)}',
                'status_message': 'Error checking status'
            })
            return flow.to_dict()

    def stop_conversion(self, flow_id: str) -> dict:
        """
        Cancel a running conversion.

        Args:
            flow_id: Flow identifier

        Returns:
            dict with success status
        """
        flow = self.delta_service.get_flow(flow_id)
        if not flow or not flow.databricks_run_id:
            return {'success': False, 'error': 'No running job found'}

        try:
            w = WorkspaceClient()
            w.jobs.cancel_run(run_id=int(flow.databricks_run_id))

            self.delta_service.update_flow_status(flow_id, {
                'status': 'NEEDS_ATTENTION',
                'status_message': 'Cancelled by user'
            })

            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_flow_notebooks(self, flow_id: str) -> List[str]:
        """
        Get list of generated notebooks for a flow.

        Args:
            flow_id: Flow identifier

        Returns:
            List of notebook paths
        """
        flow = self.delta_service.get_flow(flow_id)
        if not flow:
            return []

        return flow.generated_notebooks or []
