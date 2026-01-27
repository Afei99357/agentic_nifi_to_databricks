"""
Data model for migration_sast_results table.
Represents security scan results.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationSastResult:
    """Represents a row from migration_sast_results Delta table."""

    run_id: str                                 # PRIMARY KEY (composite) - FK to migration_runs.run_id
    iteration_num: int                          # PRIMARY KEY (composite)
    artifact_ref: str                           # PRIMARY KEY (composite) - Which artifact was scanned
    scan_status: str                            # PENDING | SCANNING | PASSED | BLOCKED

    # Optional fields
    findings_json: Optional[str] = None         # Full SAST report as JSON
    ts: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationSastResult':
        """Create MigrationSastResult from dictionary (for SQL Connector results)."""
        return cls(
            run_id=data['run_id'],
            iteration_num=data['iteration_num'],
            artifact_ref=data['artifact_ref'],
            scan_status=data['scan_status'],
            findings_json=data.get('findings_json'),
            ts=data.get('ts')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationSastResult(
            run_id=row['run_id'],
            iteration_num=row['iteration_num'],
            artifact_ref=row['artifact_ref'],
            scan_status=row['scan_status'],
            findings_json=row.get('findings_json'),
            ts=row.get('ts')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'run_id': self.run_id,
            'iteration_num': self.iteration_num,
            'artifact_ref': self.artifact_ref,
            'scan_status': self.scan_status,
            'findings_json': self.findings_json,
            'ts': self.ts.isoformat() if self.ts else None
        }
