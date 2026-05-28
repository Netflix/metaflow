from metaflow import (
    Config,
    FlowSpec,
    Parameter,
    config_expr,
    current,
    environment,
    project,
    pypi_base,
    req_parser,
    step,
)

default_config = {"project_name": "config_parser"}


def trigger_name_func(ctx):
    return [current.project_flow_name + "Trigger"]


@project(name=config_expr("cfg.project_name"))
@pypi_base(**config_expr("req_config"))
class ConfigParser(FlowSpec):

    trigger_param = Parameter(
        "trigger_param",
        default="",
        external_trigger=True,
        external_artifact=trigger_name_func,
    )
    cfg = Config("cfg", default_value=default_config)

    req_config = Config(
        "req_config", default="config_parser_requirements.txt", parser=req_parser
    )

    @step
    def start(self):
        import regex

        self.lib_version = regex.__version__  # Should be '2.5.148'
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigParser()
