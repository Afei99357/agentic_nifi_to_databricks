from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class NiFiFlow:
    flow_id: str
    flow_name: str
    processors: List[str] = field(default_factory=list)
    connections: List[dict] = field(default_factory=list)
    description: Optional[str] = None

    def to_dict(self):
        return {
            'flow_id': self.flow_id,
            'flow_name': self.flow_name,
            'processors': self.processors,
            'connections': self.connections,
            'description': self.description
        }


@dataclass
class Notebook:
    notebook_id: str
    notebook_name: str
    flow_id: str
    content: str
    language: str = "python"
    volume_path: Optional[str] = None
    preview_html: Optional[str] = None

    def to_dict(self):
        return {
            'notebook_id': self.notebook_id,
            'notebook_name': self.notebook_name,
            'flow_id': self.flow_id,
            'content': self.content,
            'language': self.language,
            'volume_path': self.volume_path,
            'preview_html': self.preview_html
        }
