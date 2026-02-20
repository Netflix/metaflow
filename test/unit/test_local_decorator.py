from metaflow import FlowSpec, step, local
from metaflow.decorators import _attach_decorators


class LocalTestFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.process)

    @local
    @step
    def process(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_local_decorator_prevents_batch():
    """Steps marked with @local should not receive @batch when run with --with batch."""
    # Test case for issue #350
    flow = LocalTestFlow(use_cli=False)
    # Simulate --with batch (respect_local_decorator=True)
    _attach_decorators(flow, ["batch"], respect_local_decorator=True)

    start_decos = [
        d.name for d in next(s for s in flow._steps if s.__name__ == "start").decorators
    ]
    process_decos = [
        d.name
        for d in next(s for s in flow._steps if s.__name__ == "process").decorators
    ]
    end_decos = [
        d.name for d in next(s for s in flow._steps if s.__name__ == "end").decorators
    ]

    assert "batch" in start_decos
    assert "local" in process_decos
    assert "batch" not in process_decos
    assert "batch" in end_decos


def test_local_decorator_prevents_kubernetes():
    """Steps marked with @local should not receive @kubernetes when run with --with kubernetes."""
    # Test case for issue #350
    flow = LocalTestFlow(use_cli=False)
    # Simulate --with kubernetes (respect_local_decorator=True)
    _attach_decorators(flow, ["kubernetes"], respect_local_decorator=True)

    process_decos = [
        d.name
        for d in next(s for s in flow._steps if s.__name__ == "process").decorators
    ]

    assert "kubernetes" not in process_decos


def test_local_decorator_prevents_multiple_remote_decorators():
    """@local should block both batch and kubernetes if both are present in --with."""
    # Requested in PR review
    flow = LocalTestFlow(use_cli=False)
    # Simulate --with batch --with kubernetes (respect_local_decorator=True)
    _attach_decorators(flow, ["batch", "kubernetes"], respect_local_decorator=True)

    process_decos = [
        d.name
        for d in next(s for s in flow._steps if s.__name__ == "process").decorators
    ]

    assert "batch" not in process_decos
    assert "kubernetes" not in process_decos


def test_local_decorator_allows_non_remote_decorators():
    """@local should only block remote execution decorators, not general-purpose ones like @retry."""
    # Test case for issue #350
    flow = LocalTestFlow(use_cli=False)
    # Simulate --with retry (respect_local_decorator=True)
    _attach_decorators(flow, ["retry"], respect_local_decorator=True)

    process_decos = [
        d.name
        for d in next(s for s in flow._steps if s.__name__ == "process").decorators
    ]

    assert "retry" in process_decos


def test_local_decorator_during_deployment():
    """@local should NOT block decorators during deployment (e.g. argo create)."""
    # Critical fix for deployment platforms as reported in greptile review
    flow = LocalTestFlow(use_cli=False)
    # Simulate deployment (respect_local_decorator=False, the default)
    _attach_decorators(flow, ["kubernetes"])

    process_decos = [
        d.name
        for d in next(s for s in flow._steps if s.__name__ == "process").decorators
    ]

    # Kubernetes SHOULD be present here even though @local is on the step
    # because it's required for the deployment to work.
    assert "kubernetes" in process_decos
    assert "local" in process_decos

