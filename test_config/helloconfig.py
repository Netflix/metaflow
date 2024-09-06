import os

from metaflow import (
    Config,
    FlowSpec,
    Parameter,
    environment,
    step,
    project,
    config_expr,
    eval_config,
    titus,
)


def silly_parser(s):
    k, v = s.split(":")
    return {k: v}


def param_func(ctx):
    return ctx.configs.config2.default_param2 + 1


def config_func(ctx):
    return {"val": 123}


default_config = {
    "run_on_titus": ["hello"],
    "cpu_count": 2,
    "env_to_start": "Romain",
    "magic_value": 42,
    "project_name": "hirec",
}

silly_config = "baz:awesome"


def titus_or_not(flow):
    to_replace = []
    for name, s in flow.steps:
        if name in flow.config.run_on_titus:
            to_replace.append((name, titus(cpu=flow.config.cpu_count)(s)))
    for name, val in to_replace:
        setattr(flow, name, val)
    return flow


def add_env_to_start(flow):
    # Add a decorator directly to a step
    flow.start = environment(vars={"hello": config_expr("config").env_to_start})(
        flow.start
    )
    return flow


@eval_config(titus_or_not)
@add_env_to_start
@project(name=config_expr("config").project_name)
class HelloConfig(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """

    default_from_config = Parameter(
        "default_from_config", default=config_expr("config2").default_param, type=int
    )

    default_from_func = Parameter("default_from_func", default=param_func, type=int)

    config = Config("config", default=default_config, help="Help for config")
    sconfig = Config(
        "sconfig",
        default="sillyconfig.txt",
        parser=silly_parser,
        help="Help for sconfig",
        required=True,
    )
    config2 = Config("config2", required=True)

    config3 = Config("config3", default=config_func)

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloConfig is %s (should be awesome)" % self.sconfig.baz)
        print(
            "Environment variable hello %s (should be Romain)" % os.environ.get("hello")
        )

        print(
            "Parameters are: default_from_config: %s, default_from_func: %s"
            % (self.default_from_config, self.default_from_func)
        )

        print("Config3 has value: %s" % self.config3.val)
        self.next(self.hello)

    @environment(
        vars={
            "normal": config.env_to_start,
            "stringify": config_expr("str(config.magic_value)"),
        }
    )
    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        print(
            "In this step, we got a normal variable %s, one that is stringified %s"
            % (
                os.environ.get("normal"),
                os.environ.get("stringify"),
            )
        )
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done")


if __name__ == "__main__":
    HelloConfig()
