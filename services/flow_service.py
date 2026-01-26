"""
Business logic for NiFi flow conversion operations.
"""

import os
import logging
from typing import List, Optional
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from services.delta_service import DeltaService
from services.dynamic_job_service import DynamicJobService


class FlowService:
    """Handles flow conversion lifecycle and Databricks job orchestration."""

    def __init__(self, delta_service: DeltaService = None):
        """
        Initialize flow service.

        Args:
            delta_service: DeltaService instance. If None, will be created.
        """
        self.delta_service = delta_service or DeltaService()
        self.dynamic_job_service = DynamicJobService()
        self.logger = logging.getLogger(__name__)

    def start_conversion(self, flow_id: str) -> dict:
        """
        Kick off conversion for a flow by creating a new Databricks job.

        Args:
            flow_id: Flow identifier

        Returns:
            dict with keys: success (bool), job_id (str), run_id (str),
                           attempt_id (str), flow_id (str), error (str)
        """
        # Get flow details
        flow = self.delta_service.get_flow(flow_id)
        if not flow:
            return {'success': False, 'error': 'Flow not found'}

        # Check if already converting
        if flow.status == 'CONVERTING':
            current_attempt = self.delta_service.get_current_attempt(flow_id)
            return {
                'success': False,
                'error': 'Conversion already in progress',
                'current_attempt_id': current_attempt.get('attempt_id') if current_attempt else None
            }

        # Validate XML path exists
        if not flow.nifi_xml_path:
            return {'success': False, 'error': 'NiFi XML path not configured for this flow'}

        try:
            # Create attempt record first (to get attempt_id)
            # Use placeholder job_id until we create the actual job
            attempt_id = self.delta_service.create_conversion_attempt(
                flow_id,
                "PENDING",
                f"NiFi_Conversion_{flow.flow_name}"
            )

            # Create and run the Databricks job
            job_info = self.dynamic_job_service.create_and_run_job(
                flow_id=flow_id,
                nifi_xml_path=flow.nifi_xml_path,
                attempt_id=attempt_id,
                flow_name=flow.flow_name
            )

            # Update attempt with actual job_id and run_id
            self.delta_service.update_attempt_status(attempt_id, {
                'databricks_job_id': job_info['job_id'],
                'databricks_run_id': job_info['run_id'],
                'job_name': job_info['job_name'],
                'status': 'RUNNING'
            })

            self.logger.info(f"Started conversion for {flow_id}: job {job_info['job_id']}, run {job_info['run_id']}")

            return {
                'success': True,
                'job_id': job_info['job_id'],
                'run_id': job_info['run_id'],
                'attempt_id': attempt_id,
                'flow_id': flow_id
            }

        except Exception as e:
            self.logger.error(f"Failed to start conversion for {flow_id}: {e}")

            # Mark attempt as failed if it was created
            if 'attempt_id' in locals():
                self.delta_service.update_attempt_status(attempt_id, {
                    'status': 'FAILED',
                    'error_message': str(e),
                    'completed_at': datetime.now().isoformat()
                })

            # Update flow status
            self.delta_service.update_flow_status(flow_id, {
                'status': 'NEEDS_ATTENTION',
                'error_message': str(e)
            })

            return {'success': False, 'error': str(e), 'attempt_id': attempt_id if 'attempt_id' in locals() else None}

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
        Poll Databricks for current status and update history table.

        Args:
            flow_id: Flow identifier

        Returns:
            Updated flow as dictionary
        """
        flow = self.delta_service.get_flow(flow_id)
        if not flow:
            return {}

        # Get current attempt
        current_attempt = self.delta_service.get_current_attempt(flow_id)
        if not current_attempt or not current_attempt.get('databricks_run_id'):
            return flow.to_dict()

        # If already terminal, return cached
        if current_attempt['status'] in ['SUCCESS', 'FAILED', 'CANCELLED']:
            return flow.to_dict()

        # Poll Databricks
        w = WorkspaceClient()
        try:
            run = w.jobs.get_run(run_id=int(current_attempt['databricks_run_id']))
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
                    updates['status'] = 'SUCCESS'
                    updates['progress_percentage'] = 100
                    updates['completed_at'] = datetime.now().isoformat()
                    updates['status_message'] = 'Conversion completed'

                    # Update flow status
                    self.delta_service.update_flow_status(flow_id, {
                        'status': 'DONE',
                        'successful_conversions': (flow.successful_conversions or 0) + 1
                    })

                elif result_state == 'FAILED':
                    updates['status'] = 'FAILED'
                    updates['error_message'] = run.state.state_message or 'Job failed'
                    updates['completed_at'] = datetime.now().isoformat()

                    # Update flow status
                    self.delta_service.update_flow_status(flow_id, {
                        'status': 'NEEDS_ATTENTION'
                    })

                else:
                    # CANCELED or other terminal states
                    updates['status'] = 'CANCELLED'
                    updates['completed_at'] = datetime.now().isoformat()

                    # Update flow status
                    self.delta_service.update_flow_status(flow_id, {
                        'status': 'NEEDS_ATTENTION'
                    })

            # Apply updates to history if any
            if updates:
                self.delta_service.update_attempt_status(current_attempt['attempt_id'], updates)

            # Return updated flow
            return self.delta_service.get_flow(flow_id).to_dict()

        except Exception as e:
            self.logger.error(f"Failed to poll status for {flow_id}: {e}")
            self.delta_service.update_attempt_status(current_attempt['attempt_id'], {
                'status': 'FAILED',
                'error_message': f'Polling failed: {str(e)}',
                'completed_at': datetime.now().isoformat()
            })
            self.delta_service.update_flow_status(flow_id, {
                'status': 'NEEDS_ATTENTION',
                'error_message': f'Polling failed: {str(e)}'
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
        # Get current attempt
        current_attempt = self.delta_service.get_current_attempt(flow_id)
        if not current_attempt or not current_attempt.get('databricks_run_id'):
            return {'success': False, 'error': 'No running job found'}

        try:
            w = WorkspaceClient()
            w.jobs.cancel_run(run_id=int(current_attempt['databricks_run_id']))

            # Update attempt status
            self.delta_service.update_attempt_status(current_attempt['attempt_id'], {
                'status': 'CANCELLED',
                'status_message': 'Cancelled by user',
                'completed_at': datetime.now().isoformat()
            })

            # Update flow status
            self.delta_service.update_flow_status(flow_id, {
                'status': 'NEEDS_ATTENTION',
                'status_message': 'Cancelled by user'
            })

            return {'success': True}
        except Exception as e:
            self.logger.error(f"Failed to cancel conversion for {flow_id}: {e}")
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
