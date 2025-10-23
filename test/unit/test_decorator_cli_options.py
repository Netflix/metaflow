def test_project_decorator_options_with_early_cli_import():
    """
    Test that @project decorator options appear when metaflow.cli is imported early.
    """
    from metaflow.cli import echo_always  # noqa: F401
    from metaflow import FlowSpec, project, step, decorators
    from metaflow._vendor import click
    from metaflow.parameters import flow_context

    @project(name="test_project")
    class TestFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    @click.command()
    def mock_cmd():
        pass

    cmd = decorators.add_decorator_options(mock_cmd)

    with flow_context(TestFlow):
        ctx = click.Context(cmd)
        params = cmd.get_params(ctx)
        param_names = [p.name for p in params]

        assert "branch" in param_names
        assert "production" in param_names
