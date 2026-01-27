"""
Data model for migration_flows table.
Represents a NiFi flow to be migrated to Databricks.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class MigrationFlow:
    """Represents a row from migration_flows Delta table."""

    flow_id: str                                # PRIMARY KEY - UUID from XML <rootGroup id="...">
    flow_name: str                              # XML filename (for reference)
    migration_status: str                       # NOT_STARTED | RUNNING | NEEDS_HUMAN | FAILED | DONE | STOPPED

    # Optional fields
    current_run_id: Optional[str] = None        # FK to migration_runs.run_id
    last_update_ts: Optional[datetime] = None

    # Optional metadata (minimal v0 per Cori's guidance)
    server: Optional[str] = None                # NiFi server/environment
    nifi_xml_path: Optional[str] = None         # Path to XML in UC volume
    description: Optional[str] = None           # Flow description
    priority: Optional[str] = None              # P0, P1, P2
    owner: Optional[str] = None                 # Team/pod responsible

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationFlow':
        """Create MigrationFlow from dictionary (for SQL Connector results)."""
        return cls(
            flow_id=data['flow_id'],
            flow_name=data['flow_name'],
            migration_status=data['migration_status'],
            current_run_id=data.get('current_run_id'),
            last_update_ts=data.get('last_update_ts'),
            server=data.get('server'),
            nifi_xml_path=data.get('nifi_xml_path'),
            description=data.get('description'),
            priority=data.get('priority'),
            owner=data.get('owner')
        )

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return MigrationFlow(
            flow_id=row['flow_id'],
            flow_name=row['flow_name'],
            migration_status=row['migration_status'],
            current_run_id=row.get('current_run_id'),
            last_update_ts=row.get('last_update_ts'),
            server=row.get('server'),
            nifi_xml_path=row.get('nifi_xml_path'),
            description=row.get('description'),
            priority=row.get('priority'),
            owner=row.get('owner')
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        return {
            'flow_id': self.flow_id,
            'flow_name': self.flow_name,
            'migration_status': self.migration_status,
            'current_run_id': self.current_run_id,
            'last_update_ts': self.last_update_ts.isoformat() if self.last_update_ts else None,
            'server': self.server,
            'nifi_xml_path': self.nifi_xml_path,
            'description': self.description,
            'priority': self.priority,
            'owner': self.owner
        }
