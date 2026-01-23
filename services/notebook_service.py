import os
import zipfile
import io
from typing import List, Optional
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter
from models.nifi_flow import Notebook
from utils.databricks_client import DatabricksClientWrapper


class NotebookPreview:
    def __init__(self, notebook_name: str, content: str, highlighted_html: str):
        self.notebook_name = notebook_name
        self.content = content
        self.highlighted_html = highlighted_html


class NotebookService:
    """Service for managing and downloading notebooks."""

    def __init__(self, databricks_client: DatabricksClientWrapper):
        self.client = databricks_client

    def get_generated_notebooks(self, volume_path: str, notebook_names: List[str]) -> List[Notebook]:
        """
        Retrieve generated notebooks from Unity Catalog volume.

        Args:
            volume_path: The base volume path where notebooks are stored
            notebook_names: List of notebook file names

        Returns:
            List of Notebook objects
        """
        notebooks = []
        for idx, name in enumerate(notebook_names):
            full_path = os.path.join(volume_path, name)
            try:
                content = self.client.read_file(full_path).decode('utf-8')
                notebook = Notebook(
                    notebook_id=f"nb_{idx}",
                    notebook_name=name,
                    flow_id=f"flow_{idx}",
                    content=content,
                    language="python",
                    volume_path=full_path
                )
                notebooks.append(notebook)
            except Exception as e:
                # If file doesn't exist, create a placeholder
                notebook = Notebook(
                    notebook_id=f"nb_{idx}",
                    notebook_name=name,
                    flow_id=f"flow_{idx}",
                    content=f"# Notebook not yet available\n# {str(e)}",
                    language="python",
                    volume_path=full_path
                )
                notebooks.append(notebook)

        return notebooks

    def save_notebooks_to_volume(self, notebooks: List[Notebook], volume_path: str) -> bool:
        """
        Save notebooks to Unity Catalog volume.

        Args:
            notebooks: List of Notebook objects to save
            volume_path: The base volume path where notebooks should be stored

        Returns:
            True if successful
        """
        try:
            for notebook in notebooks:
                full_path = os.path.join(volume_path, notebook.notebook_name)
                self.client.write_file(full_path, notebook.content.encode('utf-8'))
                notebook.volume_path = full_path
            return True
        except Exception as e:
            raise Exception(f"Failed to save notebooks: {str(e)}")

    def get_notebook_preview(self, notebook: Notebook) -> NotebookPreview:
        """
        Get a syntax-highlighted preview of a notebook.

        Args:
            notebook: The Notebook object

        Returns:
            NotebookPreview with highlighted HTML
        """
        try:
            # Use Pygments to highlight the code
            lexer = PythonLexer()
            formatter = HtmlFormatter(
                style='monokai',
                linenos='table',
                cssclass='source',
                linenostart=1
            )
            highlighted = highlight(notebook.content, lexer, formatter)

            return NotebookPreview(
                notebook_name=notebook.notebook_name,
                content=notebook.content,
                highlighted_html=highlighted
            )
        except Exception as e:
            # Fallback to plain text
            return NotebookPreview(
                notebook_name=notebook.notebook_name,
                content=notebook.content,
                highlighted_html=f"<pre>{notebook.content}</pre>"
            )

    def download_notebook(self, notebook: Notebook) -> bytes:
        """
        Get notebook content as bytes for download.

        Args:
            notebook: The Notebook object

        Returns:
            Notebook content as bytes
        """
        return notebook.content.encode('utf-8')

    def download_all_notebooks(self, notebooks: List[Notebook]) -> bytes:
        """
        Create a ZIP file containing all notebooks.

        Args:
            notebooks: List of Notebook objects

        Returns:
            ZIP file content as bytes
        """
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for notebook in notebooks:
                zip_file.writestr(notebook.notebook_name, notebook.content)

        zip_buffer.seek(0)
        return zip_buffer.read()

    def get_css_for_highlighting(self) -> str:
        """Get CSS styles for syntax highlighting."""
        formatter = HtmlFormatter(style='monokai')
        return formatter.get_style_defs('.source')
