from lxml import etree
from typing import List, Dict
from models.nifi_flow import NiFiFlow


class ValidationResult:
    def __init__(self, is_valid: bool, message: str = ""):
        self.is_valid = is_valid
        self.message = message


class NiFiParser:
    """Service for parsing and validating NiFi XML files."""

    @staticmethod
    def validate_xml_structure(xml_content: str) -> ValidationResult:
        """Validate that the XML is well-formed and appears to be a NiFi flow."""
        try:
            root = etree.fromstring(xml_content.encode('utf-8'))

            # Check for NiFi-specific elements
            if root.tag not in ['flowController', 'template', 'templateDTO']:
                return ValidationResult(False, "Not a valid NiFi XML file")

            return ValidationResult(True, "Valid NiFi XML file")
        except etree.XMLSyntaxError as e:
            return ValidationResult(False, f"Invalid XML: {str(e)}")
        except Exception as e:
            return ValidationResult(False, f"Validation error: {str(e)}")

    @staticmethod
    def parse_nifi_xml(xml_content: str) -> List[NiFiFlow]:
        """Parse NiFi XML and extract flow information."""
        try:
            root = etree.fromstring(xml_content.encode('utf-8'))
            flows = []

            # Extract processor groups (these typically represent independent flows)
            processor_groups = root.xpath('.//processGroup')

            if not processor_groups:
                # If no process groups, treat the entire flow as one
                flow = NiFiFlow(
                    flow_id="main_flow",
                    flow_name="Main Flow",
                    processors=[],
                    connections=[],
                    description="Main NiFi flow"
                )
                flows.append(flow)
            else:
                for idx, group in enumerate(processor_groups):
                    flow_id = group.get('id', f'flow_{idx}')
                    flow_name = group.find('.//name')

                    flow = NiFiFlow(
                        flow_id=flow_id,
                        flow_name=flow_name.text if flow_name is not None else f"Flow {idx + 1}",
                        processors=[],
                        connections=[],
                        description=f"NiFi process group: {flow_id}"
                    )
                    flows.append(flow)

            return flows
        except Exception as e:
            raise Exception(f"Failed to parse NiFi XML: {str(e)}")

    @staticmethod
    def get_xml_preview(xml_content: str, max_lines: int = 50) -> str:
        """Get a formatted preview of the XML content."""
        try:
            root = etree.fromstring(xml_content.encode('utf-8'))
            pretty_xml = etree.tostring(root, pretty_print=True, encoding='unicode')
            lines = pretty_xml.split('\n')
            if len(lines) > max_lines:
                preview = '\n'.join(lines[:max_lines])
                preview += f'\n... ({len(lines) - max_lines} more lines)'
                return preview
            return pretty_xml
        except Exception:
            # If parsing fails, return first N lines of raw content
            lines = xml_content.split('\n')
            if len(lines) > max_lines:
                preview = '\n'.join(lines[:max_lines])
                preview += f'\n... ({len(lines) - max_lines} more lines)'
                return preview
            return xml_content
