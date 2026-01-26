"""
Data model for NiFi flow records stored in Delta table.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime


@dataclass
class NiFiFlowRecord:
    """Represents a row from nifi_flows Delta table."""
    flow_id: str
    flow_name: str
    server: str

    # Optional metadata
    nifi_xml_path: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    owner: Optional[str] = None

    # Conversion status
    status: str = 'NOT_STARTED'
    databricks_job_id: Optional[str] = None  # DEPRECATED: Use history table
    databricks_run_id: Optional[str] = None  # DEPRECATED: Use history table
    progress_percentage: int = 0
    iterations: int = 0
    validation_percentage: int = 0

    # History tracking (NEW)
    current_attempt_id: Optional[str] = None
    total_attempts: int = 0
    successful_conversions: int = 0
    last_attempt_at: Optional[datetime] = None
    first_attempt_at: Optional[datetime] = None

    # Timestamps
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    conversion_started_at: Optional[datetime] = None  # DEPRECATED: Use history table
    conversion_completed_at: Optional[datetime] = None  # DEPRECATED: Use history table

    # Results
    generated_notebooks: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    status_message: Optional[str] = None

    @staticmethod
    def from_row(row):
        """Create from Spark Row."""
        return NiFiFlowRecord(
            flow_id=row['flow_id'],
            flow_name=row['flow_name'],
            server=row['server'],
            nifi_xml_path=row.get('nifi_xml_path'),
            description=row.get('description'),
            priority=row.get('priority'),
            owner=row.get('owner'),
            status=row.get('status', 'NOT_STARTED'),
            databricks_job_id=row.get('databricks_job_id'),
            databricks_run_id=row.get('databricks_run_id'),
            progress_percentage=row.get('progress_percentage', 0),
            iterations=row.get('iterations', 0),
            validation_percentage=row.get('validation_percentage', 0),
            current_attempt_id=row.get('current_attempt_id'),
            total_attempts=row.get('total_attempts', 0),
            successful_conversions=row.get('successful_conversions', 0),
            last_attempt_at=row.get('last_attempt_at'),
            first_attempt_at=row.get('first_attempt_at'),
            created_at=row.get('created_at'),
            last_updated=row.get('last_updated'),
            conversion_started_at=row.get('conversion_started_at'),
            conversion_completed_at=row.get('conversion_completed_at'),
            generated_notebooks=row.get('generated_notebooks', []),
            error_message=row.get('error_message'),
            status_message=row.get('status_message')
        )

    def to_dict(self):
        """Convert to dictionary for JSON response."""
        return {
            'flow_id': self.flow_id,
            'flow_name': self.flow_name,
            'server': self.server,
            'nifi_xml_path': self.nifi_xml_path,
            'description': self.description,
            'priority': self.priority,
            'owner': self.owner,
            'status': self.status,
            'databricks_job_id': self.databricks_job_id,
            'databricks_run_id': self.databricks_run_id,
            'progress_percentage': self.progress_percentage,
            'iterations': self.iterations,
            'validation_percentage': self.validation_percentage,
            'current_attempt_id': self.current_attempt_id,
            'total_attempts': self.total_attempts,
            'successful_conversions': self.successful_conversions,
            'last_attempt_at': self.last_attempt_at.isoformat() if self.last_attempt_at else None,
            'first_attempt_at': self.first_attempt_at.isoformat() if self.first_attempt_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'conversion_started_at': self.conversion_started_at.isoformat() if self.conversion_started_at else None,
            'conversion_completed_at': self.conversion_completed_at.isoformat() if self.conversion_completed_at else None,
            'generated_notebooks': self.generated_notebooks,
            'error_message': self.error_message,
            'status_message': self.status_message
        }
