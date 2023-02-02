from metaflow.decorators import FlowDecorator

class PackagerDecorator(FlowDecorator):
    name = "packager"
    defaults = {"generator": None}

    def flow_init(self, flow, graph, environment, flow_datastore, metadata, logger, echo, options):
        self._generator = self.attributes.get("generator")

    def path_tuples(self):
        for path_tuple in self._generator():
            yield path_tuple
