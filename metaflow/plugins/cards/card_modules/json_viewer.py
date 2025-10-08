"""
JSONViewer component for displaying JSON data with syntax highlighting and collapsible sections.
"""

from typing import Any, Optional, Union
from .card import MetaflowCardComponent, with_default_component_id
from .renderer_tools import render_safely
import json
from metaflow._vendor import yaml


class JSONViewer(MetaflowCardComponent):
    """
    A component for displaying JSON data with syntax highlighting and collapsible sections.

    This component provides a rich view of JSON data with proper formatting, syntax highlighting,
    and the ability to collapse/expand sections for better readability.

    Example:
    ```python
    from metaflow.cards import JSONViewer
    from metaflow import current

    data = {
        "user": {"name": "Alice", "age": 30},
        "items": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],
        "metadata": {"created": "2024-01-01", "version": "1.0"}
    }

    json_viewer = JSONViewer(data, collapsible=True, max_height="400px")
    current.card.append(json_viewer)
    ```

    Parameters
    ----------
    data : Any
        The data to display as JSON. Will be serialized using json.dumps().
    collapsible : bool, default True
        Whether to make the JSON viewer collapsible.
    max_height : str, optional
        Maximum height for the viewer (CSS value like "300px" or "20rem").
    show_copy_button : bool, default True
        Whether to show a copy-to-clipboard button.
    """

    type = "jsonViewer"

    REALTIME_UPDATABLE = True

    def __init__(
        self,
        data: Any,
        collapsible: bool = True,
        max_height: Optional[str] = None,
        show_copy_button: bool = True,
        title: Optional[str] = None,
    ):
        self._data = data
        self._collapsible = collapsible
        self._max_height = max_height
        self._show_copy_button = show_copy_button
        self._title = title

    def update(self, data: Any):
        """
        Update the JSON data.

        Parameters
        ----------
        data : Any
            New data to display as JSON.
        """
        self._data = data

    @with_default_component_id
    @render_safely
    def render(self):
        # Serialize data to JSON string
        try:
            if isinstance(self._data, str):
                # If already a string, try to parse and re-serialize for formatting
                try:
                    parsed = json.loads(self._data)
                    json_string = json.dumps(parsed, indent=2, ensure_ascii=False)
                except json.JSONDecodeError:
                    # If not valid JSON, treat as plain string
                    json_string = json.dumps(self._data, indent=2, ensure_ascii=False)
            else:
                json_string = json.dumps(
                    self._data, indent=2, ensure_ascii=False, default=str
                )
        except Exception as e:
            # Fallback for non-serializable objects
            json_string = json.dumps(
                {"error": f"Could not serialize data: {str(e)}"}, indent=2
            )

        data = {
            "type": self.type,
            "id": self.component_id,
            "json_string": json_string,
            "collapsible": self._collapsible,
            "show_copy_button": self._show_copy_button,
            "title": self._title or "JSON",
        }

        if self._max_height:
            data["max_height"] = self._max_height

        return data


class YAMLViewer(MetaflowCardComponent):
    """
    A component for displaying YAML data with syntax highlighting and collapsible sections.

    This component provides a rich view of YAML data with proper formatting and syntax highlighting.

    Example:
    ```python
    from metaflow.cards import YAMLViewer
    from metaflow import current

    data = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "credentials": {"username": "admin", "password": "secret"}
        },
        "features": ["auth", "logging", "monitoring"]
    }

    yaml_viewer = YAMLViewer(data, collapsible=True)
    current.card.append(yaml_viewer)
    ```

    Parameters
    ----------
    data : Any
        The data to display as YAML. Will be serialized to YAML format.
    collapsible : bool, default True
        Whether to make the YAML viewer collapsible.
    max_height : str, optional
        Maximum height for the viewer (CSS value like "300px" or "20rem").
    show_copy_button : bool, default True
        Whether to show a copy-to-clipboard button.
    """

    type = "yamlViewer"

    REALTIME_UPDATABLE = True

    def __init__(
        self,
        data: Any,
        collapsible: bool = True,
        max_height: Optional[str] = None,
        show_copy_button: bool = True,
        title: Optional[str] = None,
    ):
        self._data = data
        self._collapsible = collapsible
        self._max_height = max_height
        self._show_copy_button = show_copy_button
        self._title = title

    def update(self, data: Any):
        """
        Update the YAML data.

        Parameters
        ----------
        data : Any
            New data to display as YAML.
        """
        self._data = data

    def _to_yaml_string(self, data: Any) -> str:
        """
        Convert data to YAML string format using vendored YAML module.
        """
        try:
            if isinstance(data, str):
                # Try to parse as JSON first, then convert to YAML
                try:
                    import json

                    parsed = json.loads(data)
                    yaml_result = yaml.dump(
                        parsed, default_flow_style=False, indent=2, sort_keys=False
                    )
                    return (
                        str(yaml_result)
                        if yaml_result is not None
                        else "# Empty YAML result"
                    )
                except json.JSONDecodeError:
                    # If not JSON, return as-is
                    return data
            else:
                yaml_result = yaml.dump(
                    data, default_flow_style=False, indent=2, sort_keys=False
                )
                return (
                    str(yaml_result)
                    if yaml_result is not None
                    else "# Empty YAML result"
                )
        except Exception as e:
            # Fallback to JSON on any error
            import json

            return f"# Error converting to YAML: {str(e)}\n{json.dumps(data, indent=2, default=str)}"

    @with_default_component_id
    @render_safely
    def render(self):
        yaml_string = self._to_yaml_string(self._data)

        data = {
            "type": self.type,
            "id": self.component_id,
            "yaml_string": yaml_string,
            "collapsible": self._collapsible,
            "show_copy_button": self._show_copy_button,
            "title": self._title or "YAML",
        }

        if self._max_height:
            data["max_height"] = self._max_height

        return data
