"""
Business logic for NiFi flow conversion operations.

Updated to use new migration_* schema following Cori's architecture:
  - "The App should never 'compute' progress. It should display the latest rows."
  - App reads from migration_iterations for progress (not estimated)
  - Job writes structured data to DB
"""

import os
import logging
import json
from typing import List, Optional
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from services.migration_service import MigrationService
from services.dynamic_job_service import DynamicJobService
import config as app_config


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

            # Create and run the Databricks job (agent generation - iteration 1)
            job_info = self.dynamic_job_service.create_and_run_agent_job(
                flow_name=flow.flow_name,
                nifi_xml_path=flow.nifi_xml_path,
                run_id=run_id,  # Migration run ID
                flow_id=flow_id,  # Flow UUID from XML
                iteration_num=1  # First iteration is code generation
            )

            # Update run with actual job_id and set status to QUEUED
            self.migration_service.update_run_status(
                run_id,
                status='QUEUED',
                job_run_id=job_info['run_id']
            )

            # Add initial iteration tracking
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=1,
                step='agent_generate',
                step_status='RUNNING',
                summary=f"Agent generating notebooks (iteration 1)..."
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
        Poll Databricks for current job state and orchestrate iterative debugging loop.

        IMPORTANT: Following Cori's principle - "App never computes progress."
        Progress comes from migration_iterations table, not time-based estimates.

        This method coordinates the iterative loop:
        1. Checks latest iteration step and status
        2. If agent_generate completed → execute notebooks
        3. If execution fails and iterations < MAX → start agent fix iteration
        4. If execution fails and iterations >= MAX → mark needs_human
        5. If agent_fix_errors completed → check next_action and proceed

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

        # Poll Databricks for job state
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

                # Get latest iteration to determine next action
                latest_iteration = self.migration_service.get_latest_iteration(run.run_id)

                if result_state == 'SUCCESS':
                    # Agent job succeeded - check what step it was
                    if latest_iteration and latest_iteration.step == 'agent_generate':
                        # Agent finished generating notebooks (iteration 1)
                        # Update iteration status
                        self.migration_service.add_iteration(
                            run_id=run.run_id,
                            iteration_num=latest_iteration.iteration_num,
                            step='agent_generate',
                            step_status='COMPLETED',
                            summary='Agent completed notebook generation'
                        )

                        # Get generated notebooks from patches
                        patches = self.migration_service.get_run_patches(run.run_id)
                        notebook_paths = [patch.artifact_ref for patch in patches if patch.iteration_num == 1]

                        if notebook_paths:
                            # Execute the generated notebooks
                            self.logger.info(f"Agent generation complete. Executing {len(notebook_paths)} notebooks...")
                            exec_result = self.execute_iteration(run.run_id, 1, notebook_paths)

                            if exec_result['status'] == 'SUCCESS':
                                # All notebooks executed successfully!
                                self.migration_service.update_run_status(
                                    run.run_id,
                                    status='SUCCESS',
                                    end_ts=datetime.now()
                                )
                                self.migration_service.update_flow_status(flow_id, migration_status='DONE')
                            else:
                                # Execution failed - start iteration 2 to fix errors
                                self.logger.info("Execution failed. Starting agent iteration 2 to fix errors...")
                                self.start_agent_iteration(
                                    run_id=run.run_id,
                                    iteration_num=2,
                                    logs_path=exec_result.get('logs_path'),
                                    previous_notebooks=notebook_paths
                                )

                    elif latest_iteration and latest_iteration.step == 'agent_fix_errors':
                        # Agent finished fixing errors
                        # Update iteration status
                        self.migration_service.add_iteration(
                            run_id=run.run_id,
                            iteration_num=latest_iteration.iteration_num,
                            step='agent_fix_errors',
                            step_status='COMPLETED',
                            summary=f'Agent completed error fixing (iteration {latest_iteration.iteration_num})'
                        )

                        # Get fixed notebooks from patches for this iteration
                        patches = self.migration_service.get_run_patches(run.run_id)
                        notebook_paths = [
                            patch.artifact_ref for patch in patches
                            if patch.iteration_num == latest_iteration.iteration_num
                        ]

                        if notebook_paths:
                            # Execute the fixed notebooks
                            exec_result = self.execute_iteration(
                                run.run_id,
                                latest_iteration.iteration_num,
                                notebook_paths
                            )

                            if exec_result['status'] == 'SUCCESS':
                                # Success!
                                self.migration_service.update_run_status(
                                    run.run_id,
                                    status='SUCCESS',
                                    end_ts=datetime.now()
                                )
                                self.migration_service.update_flow_status(flow_id, migration_status='DONE')
                            else:
                                # Still failing - check if we should iterate again
                                if latest_iteration.iteration_num >= app_config.MAX_ITERATIONS:
                                    # Max iterations reached
                                    self.mark_needs_human(
                                        run.run_id,
                                        reason="max_iterations_reached",
                                        instructions=f"Agent tried {app_config.MAX_ITERATIONS} iterations but could not fix errors. Review logs at {exec_result.get('logs_path')}"
                                    )
                                else:
                                    # Try another iteration
                                    self.start_agent_iteration(
                                        run_id=run.run_id,
                                        iteration_num=latest_iteration.iteration_num + 1,
                                        logs_path=exec_result.get('logs_path'),
                                        previous_notebooks=notebook_paths
                                    )

                elif result_state == 'FAILED':
                    error_msg = db_run.state.state_message or 'Job failed'
                    self.migration_service.update_run_status(
                        run.run_id,
                        status='FAILED',
                        end_ts=datetime.now()
                    )

                    # Add failure iteration
                    if latest_iteration:
                        self.migration_service.add_iteration(
                            run_id=run.run_id,
                            iteration_num=latest_iteration.iteration_num,
                            step=latest_iteration.step,
                            step_status='FAILED',
                            error=error_msg
                        )

                    # Mark as needs human
                    self.mark_needs_human(
                        run.run_id,
                        reason="agent_job_failed",
                        instructions=f"Agent job failed: {error_msg}"
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
            self.mark_needs_human(
                run.run_id,
                reason="orchestration_error",
                instructions=f"Error during orchestration: {str(e)}"
            )
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

    def mark_needs_human(self, run_id: str, reason: str, instructions: Optional[str] = None):
        """
        Mark a run as needing human intervention.

        Args:
            run_id: Run identifier
            reason: Reason for human intervention (e.g., "max_iterations_reached", "unfixable_error")
            instructions: Optional instructions for human on what to do

        This method:
        1. Creates entry in migration_human_requests table
        2. Updates migration_runs.status to 'FAILED'
        3. Updates migration_flows.migration_status to 'NEEDS_HUMAN'
        """
        try:
            run = self.migration_service.get_run(run_id)
            if not run:
                self.logger.error(f"Cannot mark needs_human: run {run_id} not found")
                return

            # Get latest iteration number
            latest_iteration = self.migration_service.get_latest_iteration(run_id)
            iteration_num = latest_iteration.iteration_num if latest_iteration else 1

            # Create human request
            self.migration_service.create_human_request(
                run_id=run_id,
                iteration_num=iteration_num,
                reason=reason,
                instructions=instructions or f"Review logs and execution results for {run_id}",
                status="PENDING"
            )

            # Update run status to FAILED
            self.migration_service.update_run_status(
                run_id,
                status='FAILED',
                end_ts=datetime.now()
            )

            # Update flow status to NEEDS_HUMAN
            self.migration_service.update_flow_status(
                run.flow_id,
                migration_status='NEEDS_HUMAN'
            )

            # Add iteration entry
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=iteration_num + 1,
                step='needs_human',
                step_status='COMPLETED',
                summary=f'Marked as NEEDS_HUMAN: {reason}'
            )

            self.logger.info(f"Marked {run_id} as NEEDS_HUMAN: {reason}")

        except Exception as e:
            self.logger.error(f"Failed to mark {run_id} as needs_human: {e}")
            raise

    def execute_iteration(
        self,
        run_id: str,
        iteration_num: int,
        notebook_paths: List[str]
    ) -> dict:
        """
        Execute generated notebooks and store execution results.

        Args:
            run_id: Run identifier
            iteration_num: Current iteration number
            notebook_paths: List of notebook paths to execute

        Returns:
            dict with status, logs_path, and execution details

        This method:
        1. Creates execution job via DynamicJobService
        2. Tracks execution in migration_iterations table
        3. Waits for completion
        4. Stores execution result in migration_exec_results table
        """
        try:
            run = self.migration_service.get_run(run_id)
            if not run:
                raise Exception(f"Run {run_id} not found")

            flow = self.migration_service.get_flow(run.flow_id)
            logs_path = f"{app_config.LOGS_VOLUME_PATH}/{run_id}/iter{iteration_num}"

            self.logger.info(f"Starting execution for iteration {iteration_num} of {run_id}")

            # Track execution start in iterations table
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=iteration_num,
                step='execute_notebooks',
                step_status='RUNNING',
                summary=f'Executing {len(notebook_paths)} notebooks...'
            )

            # Create and run execution job
            job_info = self.dynamic_job_service.create_execution_job(
                flow_name=flow.flow_name,
                run_id=run_id,
                iteration_num=iteration_num,
                notebook_paths=notebook_paths,
                logs_path=logs_path
            )

            # Run the job
            databricks_run_id = self.dynamic_job_service.run_job(job_info['job_id'])

            # Wait for completion
            w = WorkspaceClient()
            run_start = datetime.now()

            while True:
                db_run = w.jobs.get_run(run_id=int(databricks_run_id))
                db_state = db_run.state.life_cycle_state.value

                if db_state == 'TERMINATED':
                    result_state = db_run.state.result_state.value if db_run.state.result_state else 'UNKNOWN'
                    runtime_s = int((datetime.now() - run_start).total_seconds())

                    if result_state == 'SUCCESS':
                        # Update iteration as completed
                        self.migration_service.add_iteration(
                            run_id=run_id,
                            iteration_num=iteration_num,
                            step='execute_notebooks',
                            step_status='COMPLETED',
                            summary='All notebooks executed successfully'
                        )

                        # Store execution result
                        self.migration_service.add_exec_result(
                            run_id=run_id,
                            iteration_num=iteration_num,
                            job_type='notebook_execution',
                            status='SUCCESS',
                            runtime_s=runtime_s,
                            logs_ref=logs_path
                        )

                        return {
                            'status': 'SUCCESS',
                            'logs_path': logs_path,
                            'runtime_s': runtime_s
                        }

                    else:
                        error_msg = db_run.state.state_message or 'Execution failed'

                        # Update iteration as failed
                        self.migration_service.add_iteration(
                            run_id=run_id,
                            iteration_num=iteration_num,
                            step='execute_notebooks',
                            step_status='FAILED',
                            error=error_msg
                        )

                        # Store execution result
                        self.migration_service.add_exec_result(
                            run_id=run_id,
                            iteration_num=iteration_num,
                            job_type='notebook_execution',
                            status='FAILED',
                            runtime_s=runtime_s,
                            error=error_msg,
                            logs_ref=logs_path
                        )

                        return {
                            'status': 'FAILED',
                            'logs_path': logs_path,
                            'error': error_msg,
                            'runtime_s': runtime_s
                        }

                # Wait before polling again
                import time
                time.sleep(10)

        except Exception as e:
            self.logger.error(f"Failed to execute iteration {iteration_num} for {run_id}: {e}")

            # Mark iteration as failed
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=iteration_num,
                step='execute_notebooks',
                step_status='FAILED',
                error=str(e)
            )

            return {
                'status': 'FAILED',
                'error': str(e)
            }

    def start_agent_iteration(
        self,
        run_id: str,
        iteration_num: int,
        logs_path: str,
        previous_notebooks: Optional[List[str]] = None
    ) -> dict:
        """
        Start a new agent iteration to analyze logs and fix errors.

        Args:
            run_id: Run identifier
            iteration_num: Iteration number (2+)
            logs_path: Path to execution logs from previous iteration
            previous_notebooks: Optional list of notebook paths to fix

        Returns:
            dict with job info

        This method:
        1. Tracks new iteration in migration_iterations table with step='agent_fix_errors'
        2. Launches agent job with logs_path so agent can read logs and fix errors
        """
        try:
            run = self.migration_service.get_run(run_id)
            if not run:
                raise Exception(f"Run {run_id} not found")

            flow = self.migration_service.get_flow(run.flow_id)

            self.logger.info(f"Starting agent iteration {iteration_num} for {run_id}")

            # Track iteration start
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=iteration_num,
                step='agent_fix_errors',
                step_status='RUNNING',
                summary=f'Agent analyzing logs and fixing errors (iteration {iteration_num})...'
            )

            # Launch agent job with logs
            job_info = self.dynamic_job_service.create_and_run_agent_job(
                flow_name=flow.flow_name,
                nifi_xml_path=flow.nifi_xml_path,
                run_id=run_id,
                flow_id=flow.flow_id,
                iteration_num=iteration_num,
                logs_path=logs_path,
                previous_notebooks=previous_notebooks
            )

            self.logger.info(f"Agent job started for iteration {iteration_num}: {job_info['job_id']}")

            return job_info

        except Exception as e:
            self.logger.error(f"Failed to start agent iteration {iteration_num} for {run_id}: {e}")

            # Mark iteration as failed
            self.migration_service.add_iteration(
                run_id=run_id,
                iteration_num=iteration_num,
                step='agent_fix_errors',
                step_status='FAILED',
                error=str(e)
            )

            raise
