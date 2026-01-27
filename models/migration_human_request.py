"""
Data model for migration_human_requests table.
Represents requests for human intervention.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationHumanRequest:
    """Represents a row from migration_human_requests Delta table."""

    run_id: str                                 # PRIMARY KEY (composite) - FK to migration_runs.run_id
    iteration_num: int                          # PRIMARY KEY (composite)
    reason: str                                 # "sast_blocked", "validation_failed", "manual_review"
    status: str                                 # PENDING | RESOLVED | IGNORED

    # Optional fields
    instructions: Optional[str] = None          # What human should do
    ts: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationHumanRequest':
        """Create MigrationHumanRequest from dictionary (for SQL Connector results)."""
        return cls(
            run_id=data['run_id'],
            iteration_num=data['iteration_num'],
            reason=data['reason'],
            status=data['status'],
            instructions=data.get('instructions'),
            ts=data.get('ts')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationHumanRequest(
            run_id=row['run_id'],
            iteration_num=row['iteration_num'],
            reason=row['reason'],
            status=row['status'],
            instructions=row.get('instructions'),
            ts=row.get('ts')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'run_id': self.run_id,
            'iteration_num': self.iteration_num,
            'reason': self.reason,
            'status': self.status,
            'instructions': self.instructions,
            'ts': self.ts.isoformat() if self.ts else None
        }
