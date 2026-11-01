from metaflow import FlowSpec, Run, current, get_namespace, namespace, step

# Reproduces the real user-facing scenario from #2734:
# global namespace is selected at module scope.
namespace(None)


class GlobalNamespaceFlow(FlowSpec):
    @step
    def start(self):
        # This client access is what regresses if the worker receives ""
        # instead of None for the namespace.
        self.worker_namespace = get_namespace()
        self.resolved_run_pathspec = Run(
            f"{current.flow_name}/{current.run_id}"
        ).pathspec
        self.next(self.end)

    @step
    def end(self):
        self.worker_namespace = get_namespace()
        self.resolved_run_pathspec = Run(
            f"{current.flow_name}/{current.run_id}"
        ).pathspec


if __name__ == "__main__":
    GlobalNamespaceFlow()