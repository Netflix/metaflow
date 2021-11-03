from metaflow.decorators import StepDecorator
from metaflow.unbounded_foreach import UnboundedForeachInput


class MultinodeDecorator(StepDecorator):
    name = "multinode"
    defaults = {
        "nodes": 2,
    }

    def __init__(self, attributes=None, statically_defined=False):
        self.nodes = attributes["nodes"]
        super(MultinodeDecorator, self).__init__(attributes, statically_defined)
