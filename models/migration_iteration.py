"""
Data model for migration_iterations table.
Represents step-by-step events within a migration run.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationIteration:
    """Represents a row from migration_iterations Delta table."""

    run_id: str                                 # PRIMARY KEY (composite) - FK to migration_runs.run_id
    iteration_num: int                          # PRIMARY KEY (composite)
    step: str                                   # "parse_xml", "generate_code", "validate"
    step_status: str                            # RUNNING | COMPLETED | FAILED

    # Optional fields
    ts: Optional[datetime] = None
    summary: Optional[str] = None               # Human-readable progress description
    error: Optional[str] = None                 # Error message if step_status = FAILED

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationIteration':
        """Create MigrationIteration from dictionary (for SQL Connector results)."""
        return cls(
            run_id=data['run_id'],
            iteration_num=data['iteration_num'],
            step=data['step'],
            step_status=data['step_status'],
            ts=data.get('ts'),
            summary=data.get('summary'),
            error=data.get('error')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationIteration(
            run_id=row['run_id'],
            iteration_num=row['iteration_num'],
            step=row['step'],
            step_status=row['step_status'],
            ts=row.get('ts'),
            summary=row.get('summary'),
            error=row.get('error')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'run_id': self.run_id,
            'iteration_num': self.iteration_num,
            'step': self.step,
            'step_status': self.step_status,
            'ts': self.ts.isoformat() if self.ts else None,
            'summary': self.summary,
            'error': self.error
        }
