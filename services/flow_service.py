"""
Business logic for NiFi flow conversion operations.

Updated to use new migration_* schema following Cori's architecture:
  - "The App should never 'compute' progress. It should display the latest rows."
  - App reads from migration_iterations for progress (not estimated)
  - Job writes structured data to DB
"""

import os
import logging
from typing import List, Optional
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from services.migration_service import MigrationService
from services.dynamic_job_service import DynamicJobService


class FlowService:
    """Handles flow conversion lifecycle and Databricks job orchestration."""

    def __init__(self, migration_service: MigrationService = None):
        """
        Initialize flow service.

        Args:
            migration_service: MigrationService instance. If None, will be created.
        """
        self.migration_service = migration_service or MigrationService()
        self.dynamic_job_service = DynamicJobService()
        self.logger = logging.getLogger(__name__)

    def start_conversion(self, flow_id: str) -> dict:
        """
        Kick off conversion for a flow by creating a new Databricks job.

        Args:
            flow_id: Flow identifier (UUID from XML)

        Returns:
            dict with keys: success (bool), job_id (str), run_id (str),
                           flow_id (str), error (str)
        """
        # Get flow details
        flow = self.migration_service.get_flow(flow_id)
        if not flow:
            return {'success': False, 'error': 'Flow not found'}

        # Check if already converting
        if flow.migration_status == 'RUNNING':
            return {
                'success': False,
                'error': 'Conversion already in progress',
                'current_run_id': flow.current_run_id
            }

        # Validate XML path exists
        if not flow.nifi_xml_path:
            return {'success': False, 'error': 'NiFi XML path not configured for this flow'}

        try:
            # Create run record (to get run_id)
            run_id = self.migration_service.create_run(flow_id)

            # Update flow status to RUNNING and link to current run
            self.migration_service.update_flow_status(
                flow_id,
                migration_status='RUNNING',
                current_run_id=run_id
            )

            # Create and run the Databricks job
            job_info = self.dynamic_job_service.create_and_run_job(
                flow_name=flow.flow_name,
                nifi_xml_path=flow.nifi_xml_path,
                run_id=run_id  # Pass run_id instead of attempt_id
            )

            # Update run with actual job_id and set status to QUEUED
            self.migration_service.update_run_status(
                run_id,
                status='QUEUED',
                job_run_id=job_info['run_id']
            )

            # Add initial iteration
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=1,
                step='job_created',
                step_status='COMPLETED',
                summary=f"Databricks job created: {job_info['job_id']}"
            )

            self.logger.info(f"Started conversion for {flow_id}: job {job_info['job_id']}, run {job_info['run_id']}")

            return {
                'success': True,
                'job_id': job_info['job_id'],
                'databricks_run_id': job_info['run_id'],
                'run_id': run_id,
                'flow_id': flow_id
            }

        except Exception as e:
            self.logger.error(f"Failed to start conversion for {flow_id}: {e}")

            # Mark run as failed if it was created
            if 'run_id' in locals():
                self.migration_service.update_run_status(
                    run_id,
                    status='FAILED'
                )
                self.migration_service.add_iteration(
                    run_id=run_id,
                    iteration_num=1,
                    step='job_creation',
                    step_status='FAILED',
                    error=str(e)
                )

            # Update flow status
            self.migration_service.update_flow_status(flow_id, 'NEEDS_HUMAN')

            return {'success': False, 'error': str(e), 'run_id': run_id if 'run_id' in locals() else None}

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
        Poll Databricks for current job state and read progress from DB.

        IMPORTANT: Following Cori's principle - "App never computes progress."
        Progress comes from migration_iterations table, not time-based estimates.

        Args:
            flow_id: Flow identifier (UUID)

        Returns:
            Updated flow with progress from DB
        """
        flow = self.migration_service.get_flow(flow_id)
        if not flow:
            return {}

        # Get current run
        if not flow.current_run_id:
            return flow.to_dict()

        run = self.migration_service.get_run(flow.current_run_id)
        if not run or not run.job_run_id:
            return flow.to_dict()

        # If already terminal, return cached
        if run.status in ['SUCCESS', 'FAILED', 'CANCELLED']:
            # Get latest iteration for progress info
            latest_iteration = self.migration_service.get_latest_iteration(run.run_id)
            result = flow.to_dict()
            if latest_iteration:
                result['current_step'] = latest_iteration.step
                result['current_summary'] = latest_iteration.summary
                result['current_iteration'] = latest_iteration.iteration_num
            return result

        # Poll Databricks for job state (NOT progress)
        w = WorkspaceClient()
        try:
            db_run = w.jobs.get_run(run_id=int(run.job_run_id))
            db_state = db_run.state.life_cycle_state.value

            if db_state in ['PENDING', 'RUNNING', 'TERMINATING']:
                # Update run status if changed
                if db_state == 'RUNNING' and run.status != 'RUNNING':
                    self.migration_service.update_run_status(run.run_id, status='RUNNING')
                    self.migration_service.update_flow_status(flow_id, migration_status='RUNNING')

            elif db_state == 'TERMINATED':
                result_state = db_run.state.result_state.value if db_run.state.result_state else 'UNKNOWN'

                if result_state == 'SUCCESS':
                    self.migration_service.update_run_status(
                        run.run_id,
                        status='SUCCESS',
                        end_ts=datetime.now()
                    )
                    self.migration_service.update_flow_status(flow_id, migration_status='DONE')

                elif result_state == 'FAILED':
                    error_msg = db_run.state.state_message or 'Job failed'
                    self.migration_service.update_run_status(
                        run.run_id,
                        status='FAILED',
                        end_ts=datetime.now()
                    )
                    self.migration_service.update_flow_status(flow_id, migration_status='NEEDS_HUMAN')

                    # Add failure iteration
                    latest = self.migration_service.get_latest_iteration(run.run_id)
                    next_iter = (latest.iteration_num + 1) if latest else 1
                    self.migration_service.add_iteration(
                        run_id=run.run_id,
                        iteration_num=next_iter,
                        step='job_termination',
                        step_status='FAILED',
                        error=error_msg
                    )

                else:
                    # CANCELED or other terminal states
                    self.migration_service.update_run_status(
                        run.run_id,
                        status='CANCELLED',
                        end_ts=datetime.now()
                    )
                    self.migration_service.update_flow_status(flow_id, migration_status='STOPPED')

            # Read progress from DB (NOT computed)
            latest_iteration = self.migration_service.get_latest_iteration(run.run_id)
            result = flow.to_dict()

            if latest_iteration:
                result['current_step'] = latest_iteration.step
                result['current_summary'] = latest_iteration.summary
                result['current_iteration'] = latest_iteration.iteration_num
                result['step_status'] = latest_iteration.step_status
                if latest_iteration.error:
                    result['error'] = latest_iteration.error

            return result

        except Exception as e:
            self.logger.error(f"Failed to poll status for {flow_id}: {e}")
            self.migration_service.update_run_status(run.run_id, status='FAILED', end_ts=datetime.now())
            self.migration_service.update_flow_status(flow_id, migration_status='NEEDS_HUMAN')
            return flow.to_dict()

    def stop_conversion(self, flow_id: str) -> dict:
        """
        Cancel a running conversion.

        Args:
            flow_id: Flow identifier

        Returns:
            dict with success status
        """
        # Get flow and current run
        flow = self.migration_service.get_flow(flow_id)
        if not flow or not flow.current_run_id:
            return {'success': False, 'error': 'No running job found'}

        run = self.migration_service.get_run(flow.current_run_id)
        if not run or not run.job_run_id:
            return {'success': False, 'error': 'No running job found'}

        try:
            w = WorkspaceClient()
            w.jobs.cancel_run(run_id=int(run.job_run_id))

            # Update run status
            self.migration_service.update_run_status(
                run.run_id,
                status='CANCELLED',
                end_ts=datetime.now()
            )

            # Add cancellation iteration
            latest = self.migration_service.get_latest_iteration(run.run_id)
            next_iter = (latest.iteration_num + 1) if latest else 1
            self.migration_service.add_iteration(
                run_id=run.run_id,
                iteration_num=next_iter,
                step='user_cancellation',
                step_status='COMPLETED',
                summary='Cancelled by user'
            )

            # Update flow status
            self.migration_service.update_flow_status(flow_id, migration_status='STOPPED')

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
            List of notebook paths from patches table
        """
        flow = self.migration_service.get_flow(flow_id)
        if not flow or not flow.current_run_id:
            return []

        # Get patches (generated notebooks) from current run
        patches = self.migration_service.get_run_patches(flow.current_run_id)
        return [patch.artifact_ref for patch in patches]
