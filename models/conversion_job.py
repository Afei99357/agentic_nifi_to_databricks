from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from enum import Enum


class JobStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ConversionJob:
    job_id: str
    user_id: str
    source_file: str
    source_volume_path: str
    status: JobStatus = JobStatus.PENDING
    progress_percentage: int = 0
    status_message: str = "Job created"
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    generated_notebooks: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    def to_dict(self):
        return {
            'job_id': self.job_id,
            'user_id': self.user_id,
            'source_file': self.source_file,
            'source_volume_path': self.source_volume_path,
            'status': self.status.value,
            'progress_percentage': self.progress_percentage,
            'status_message': self.status_message,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'generated_notebooks': self.generated_notebooks,
            'error_message': self.error_message,
            'warnings': self.warnings
        }


@dataclass
class TaskStatus:
    task_key: str
    notebook_path: str
    state: str  # 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'TIMEOUT', 'CANCELLED'
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None

    def to_dict(self):
        return {
            'task_key': self.task_key,
            'notebook_path': self.notebook_path,
            'state': self.state,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message
        }


@dataclass
class JobRunStatus:
    run_id: str
    state: str  # 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED'
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: int = 0
    tasks: List[TaskStatus] = field(default_factory=list)

    def to_dict(self):
        return {
            'run_id': self.run_id,
            'state': self.state,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'tasks': [task.to_dict() for task in self.tasks]
        }
