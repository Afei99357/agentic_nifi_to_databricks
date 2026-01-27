"""
Data model for migration_runs table.
Represents one orchestrator run for a flow migration.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationRun:
    """Represents a row from migration_runs Delta table."""

    run_id: str                                 # PRIMARY KEY - Format: "{flow_id}_run_{timestamp}"
    flow_id: str                                # FK to migration_flows.flow_id
    status: str                                 # CREATING | QUEUED | RUNNING | SUCCESS | FAILED | CANCELLED

    # Optional fields
    job_run_id: Optional[str] = None            # Databricks run ID
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationRun':
        """Create MigrationRun from dictionary (for SQL Connector results)."""
        return cls(
            run_id=data['run_id'],
            flow_id=data['flow_id'],
            status=data['status'],
            job_run_id=data.get('job_run_id'),
            start_ts=data.get('start_ts'),
            end_ts=data.get('end_ts')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationRun(
            run_id=row['run_id'],
            flow_id=row['flow_id'],
            status=row['status'],
            job_run_id=row.get('job_run_id'),
            start_ts=row.get('start_ts'),
            end_ts=row.get('end_ts')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'run_id': self.run_id,
            'flow_id': self.flow_id,
            'status': self.status,
            'job_run_id': self.job_run_id,
            'start_ts': self.start_ts.isoformat() if self.start_ts else None,
            'end_ts': self.end_ts.isoformat() if self.end_ts else None
        }
