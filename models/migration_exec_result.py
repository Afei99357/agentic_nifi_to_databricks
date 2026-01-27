"""
Data model for migration_exec_results table.
Represents job execution results.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationExecResult:
    """Represents a row from migration_exec_results Delta table."""

    run_id: str                                 # PRIMARY KEY (composite) - FK to migration_runs.run_id
    iteration_num: int                          # PRIMARY KEY (composite)
    job_type: str                               # PRIMARY KEY (composite) - "flow_job" | "validation_job"
    status: str                                 # SUCCESS | FAILED | TIMEOUT

    # Optional fields
    runtime_s: Optional[int] = None
    error: Optional[str] = None
    logs_ref: Optional[str] = None              # Path to log file in UC Volume
    ts: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationExecResult':
        """Create MigrationExecResult from dictionary (for SQL Connector results)."""
        return cls(
            run_id=data['run_id'],
            iteration_num=data['iteration_num'],
            job_type=data['job_type'],
            status=data['status'],
            runtime_s=data.get('runtime_s'),
            error=data.get('error'),
            logs_ref=data.get('logs_ref'),
            ts=data.get('ts')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationExecResult(
            run_id=row['run_id'],
            iteration_num=row['iteration_num'],
            job_type=row['job_type'],
            status=row['status'],
            runtime_s=row.get('runtime_s'),
            error=row.get('error'),
            logs_ref=row.get('logs_ref'),
            ts=row.get('ts')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'run_id': self.run_id,
            'iteration_num': self.iteration_num,
            'job_type': self.job_type,
            'status': self.status,
            'runtime_s': self.runtime_s,
            'error': self.error,
            'logs_ref': self.logs_ref,
            'ts': self.ts.isoformat() if self.ts else None
        }
