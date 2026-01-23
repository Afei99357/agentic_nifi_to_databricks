import os
from typing import List, Dict
from datetime import datetime
from utils.databricks_client import DatabricksClientWrapper
from services.nifi_parser import NiFiParser, ValidationResult


class FileMetadata:
    def __init__(self, name: str, path: str, size: int, modified: datetime, is_directory: bool = False):
        self.name = name
        self.path = path
        self.size = size
        self.modified = modified
        self.is_directory = is_directory

    def to_dict(self):
        return {
            'name': self.name,
            'path': self.path,
            'size': self.size,
            'size_mb': round(self.size / (1024 * 1024), 2) if self.size > 0 else 0,
            'modified': self.modified.isoformat() if self.modified else None,
            'is_directory': self.is_directory
        }


class Volume:
    def __init__(self, name: str, catalog: str, schema: str, volume_type: str):
        self.name = name
        self.catalog = catalog
        self.schema = schema
        self.volume_type = volume_type
        self.full_path = f"/Volumes/{catalog}/{schema}/{name}"

    def to_dict(self):
        return {
            'name': self.name,
            'catalog': self.catalog,
            'schema': self.schema,
            'volume_type': self.volume_type,
            'full_path': self.full_path
        }


class UnityCatalogService:
    """Service for Unity Catalog volume file operations."""

    def __init__(self, databricks_client: DatabricksClientWrapper):
        self.client = databricks_client

    def list_volumes(self, catalog: str, schema: str) -> List[Volume]:
        """List all volumes in a catalog and schema."""
        try:
            volumes_list = self.client.list_volumes(catalog, schema)
            volumes = []
            for vol in volumes_list:
                volume = Volume(
                    name=vol.name,
                    catalog=vol.catalog_name,
                    schema=vol.schema_name,
                    volume_type=vol.volume_type.value if vol.volume_type else "MANAGED"
                )
                volumes.append(volume)
            return volumes
        except Exception as e:
            raise Exception(f"Failed to list volumes: {str(e)}")

    def list_files(self, volume_path: str, filter_xml: bool = True) -> List[FileMetadata]:
        """List files in a Unity Catalog volume path."""
        try:
            # Use Databricks Files API for Unity Catalog volumes
            directory_entries = self.client.list_files_in_volume(volume_path)

            files = []
            for entry in directory_entries:
                is_directory = entry.is_directory

                # Filter for XML files if requested
                if filter_xml and not is_directory and not entry.name.lower().endswith('.xml'):
                    continue

                file_meta = FileMetadata(
                    name=entry.name,
                    path=entry.path,
                    size=entry.file_size if entry.file_size else 0,
                    modified=datetime.fromtimestamp(entry.last_modified / 1000) if entry.last_modified else datetime.now(),
                    is_directory=is_directory
                )
                files.append(file_meta)

            # Sort: directories first, then by name
            files.sort(key=lambda x: (not x.is_directory, x.name.lower()))
            return files
        except Exception as e:
            raise Exception(f"Failed to list files: {str(e)}")

    def read_nifi_xml(self, file_path: str) -> str:
        """Read NiFi XML file content."""
        try:
            content = self.client.read_file(file_path)
            return content.decode('utf-8')
        except Exception as e:
            raise Exception(f"Failed to read NiFi XML: {str(e)}")

    def validate_xml_structure(self, xml_content: str) -> ValidationResult:
        """Validate XML structure using NiFiParser."""
        return NiFiParser.validate_xml_structure(xml_content)

    def write_file(self, file_path: str, content: str):
        """Write content to a file in Unity Catalog volume."""
        try:
            self.client.write_file(file_path, content.encode('utf-8'))
        except Exception as e:
            raise Exception(f"Failed to write file: {str(e)}")
