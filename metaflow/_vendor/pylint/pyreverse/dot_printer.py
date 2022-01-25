# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""
Class to generate files in dot format and image formats supported by Graphviz.
"""
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.pyreverse.printer import EdgeType, Layout, NodeProperties, NodeType, Printer
from metaflow._vendor.pylint.pyreverse.utils import check_graphviz_availability, get_annotation_label

ALLOWED_CHARSETS: FrozenSet[str] = frozenset(("utf-8", "iso-8859-1", "latin1"))
SHAPES: Dict[NodeType, str] = {
    NodeType.PACKAGE: "box",
    NodeType.INTERFACE: "record",
    NodeType.CLASS: "record",
}
ARROWS: Dict[EdgeType, Dict[str, str]] = {
    EdgeType.INHERITS: dict(arrowtail="none", arrowhead="empty"),
    EdgeType.IMPLEMENTS: dict(arrowtail="node", arrowhead="empty", style="dashed"),
    EdgeType.ASSOCIATION: dict(
        fontcolor="green", arrowtail="none", arrowhead="diamond", style="solid"
    ),
    EdgeType.USES: dict(arrowtail="none", arrowhead="open"),
}


class DotPrinter(Printer):
    DEFAULT_COLOR = "black"

    def __init__(
        self,
        title: str,
        layout: Optional[Layout] = None,
        use_automatic_namespace: Optional[bool] = None,
    ):
        layout = layout or Layout.BOTTOM_TO_TOP
        self.charset = "utf-8"
        super().__init__(title, layout, use_automatic_namespace)

    def _open_graph(self) -> None:
        """Emit the header lines"""
        self.emit(f'digraph "{self.title}" {{')
        if self.layout:
            self.emit(f"rankdir={self.layout.value}")
        if self.charset:
            assert (
                self.charset.lower() in ALLOWED_CHARSETS
            ), f"unsupported charset {self.charset}"
            self.emit(f'charset="{self.charset}"')

    def emit_node(
        self,
        name: str,
        type_: NodeType,
        properties: Optional[NodeProperties] = None,
    ) -> None:
        """Create a new node. Nodes can be classes, packages, participants etc."""
        if properties is None:
            properties = NodeProperties(label=name)
        shape = SHAPES[type_]
        color = properties.color if properties.color is not None else self.DEFAULT_COLOR
        style = "filled" if color != self.DEFAULT_COLOR else "solid"
        label = self._build_label_for_node(properties)
        label_part = f', label="{label}"' if label else ""
        fontcolor_part = (
            f', fontcolor="{properties.fontcolor}"' if properties.fontcolor else ""
        )
        self.emit(
            f'"{name}" [color="{color}"{fontcolor_part}{label_part}, shape="{shape}", style="{style}"];'
        )

    def _build_label_for_node(
        self, properties: NodeProperties, is_interface: Optional[bool] = False
    ) -> str:
        if not properties.label:
            return ""

        label: str = properties.label
        if is_interface:
            # add a stereotype
            label = "<<interface>>\\n" + label

        if properties.attrs is None and properties.methods is None:
            # return a "compact" form which only displays the class name in a box
            return label

        # Add class attributes
        attrs: List[str] = properties.attrs or []
        label = "{" + label + "|" + r"\l".join(attrs) + r"\l|"

        # Add class methods
        methods: List[nodes.FunctionDef] = properties.methods or []
        for func in methods:
            args = self._get_method_arguments(func)
            label += fr"{func.name}({', '.join(args)})"
            if func.returns:
                label += ": " + get_annotation_label(func.returns)
            label += r"\l"
        label += "}"
        return label

    def emit_edge(
        self,
        from_node: str,
        to_node: str,
        type_: EdgeType,
        label: Optional[str] = None,
    ) -> None:
        """Create an edge from one node to another to display relationships."""
        arrowstyle = ARROWS[type_]
        attrs = [f'{prop}="{value}"' for prop, value in arrowstyle.items()]
        if label:
            attrs.append(f'label="{label}"')
        self.emit(f'"{from_node}" -> "{to_node}" [{", ".join(sorted(attrs))}];')

    def generate(self, outputfile: str) -> None:
        self._close_graph()
        graphviz_extensions = ("dot", "gv")
        name = self.title
        if outputfile is None:
            target = "png"
            pdot, dot_sourcepath = tempfile.mkstemp(".gv", name)
            ppng, outputfile = tempfile.mkstemp(".png", name)
            os.close(pdot)
            os.close(ppng)
        else:
            target = Path(outputfile).suffix.lstrip(".")
            if not target:
                target = "png"
                outputfile = outputfile + "." + target
            if target not in graphviz_extensions:
                pdot, dot_sourcepath = tempfile.mkstemp(".gv", name)
                os.close(pdot)
            else:
                dot_sourcepath = outputfile
        with open(dot_sourcepath, "w", encoding="utf8") as outfile:
            outfile.writelines(self.lines)
        if target not in graphviz_extensions:
            check_graphviz_availability()
            use_shell = sys.platform == "win32"
            subprocess.call(
                ["dot", "-T", target, dot_sourcepath, "-o", outputfile],
                shell=use_shell,
            )
            os.unlink(dot_sourcepath)

    def _close_graph(self) -> None:
        """Emit the lines needed to properly close the graph."""
        self.emit("}\n")
