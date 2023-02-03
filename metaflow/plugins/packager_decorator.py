import os
from pathlib import Path
from metaflow.metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from metaflow.decorators import FlowDecorator

class PackagerDecorator(FlowDecorator):
    name = "packager"
    defaults = {"generator": None, "content": None, "suffixes": DEFAULT_PACKAGE_SUFFIXES.split(",")}

    def flow_init(self, flow, graph, environment, flow_datastore, metadata, logger, echo, options):
        self._content  = self.attributes.get("content")
        self._suffixes = self.attributes.get("suffixes")

    def path_tuples(self, flowdir):
        _flowdir = Path(flowdir)
        if type(self._content) in { list, tuple, set, frozenset }:
            for path in self._content:
                source   = (_flowdir / path).resolve()
                target   = str(source.relative_to(_flowdir)) if source.is_relative_to(_flowdir) else source.parts[-1]
                suffixes = self._suffixes if os.path.isdir(source) else None
                yield (suffixes, str(source), target)
        elif type(self._content) is dict:
            for path, target in self._content.items():
                source   = (_flowdir / path).resolve()
                suffixes = self._suffixes if os.path.isdir(source) else None
                yield (suffixes, str(source), target)
