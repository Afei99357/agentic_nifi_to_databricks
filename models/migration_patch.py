"""
Data model for migration_patches table.
Represents code changes made by the agent.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationPatch:
    """Represents a row from migration_patches Delta table."""

    run_id: str                                 # PRIMARY KEY (composite) - FK to migration_runs.run_id
    iteration_num: int                          # PRIMARY KEY (composite)
    artifact_ref: str                           # PRIMARY KEY (composite) - Which file/artifact was modified

    # Optional fields
    patch_summary: Optional[str] = None         # Human-readable description
    diff_ref: Optional[str] = None              # Path to diff file or Git commit
    ts: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationPatch':
        """Create MigrationPatch from dictionary (for SQL Connector results)."""
        return cls(
            run_id=data['run_id'],
            iteration_num=data['iteration_num'],
            artifact_ref=data['artifact_ref'],
            patch_summary=data.get('patch_summary'),
            diff_ref=data.get('diff_ref'),
            ts=data.get('ts')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationPatch(
            run_id=row['run_id'],
            iteration_num=row['iteration_num'],
            artifact_ref=row['artifact_ref'],
            patch_summary=row.get('patch_summary'),
            diff_ref=row.get('diff_ref'),
            ts=row.get('ts')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'run_id': self.run_id,
            'iteration_num': self.iteration_num,
            'artifact_ref': self.artifact_ref,
            'patch_summary': self.patch_summary,
            'diff_ref': self.diff_ref,
            'ts': self.ts.isoformat() if self.ts else None
        }
