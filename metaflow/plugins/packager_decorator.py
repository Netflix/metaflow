import os
from pathlib import Path
from metaflow.metaflow_config import DEFAULT_PACKAGE_SUFFIXES
from metaflow.decorators import FlowDecorator

class PackagerDecorator(FlowDecorator):
    name = "packager"
    defaults = {
        "root": None
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.root = self.attributes.get("root")
