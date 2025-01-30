import json
import os

from metaflow import (
    Config,
    FlowSpec,
    Parameter,
    config_expr,
    current,
    environment,
    project,
    step,
    timeout,
)

default_config = {"blur": 123, "timeout": 10}


def myparser(s: str):
    return {"hi": "you"}


class ConfigSimple(FlowSpec):

    cfg = Config("cfg", default_value=default_config)
    cfg_req = Config("cfg_req2", required=True)
    blur = Parameter("blur", default=cfg.blur)
    blur2 = Parameter("blur2", default=cfg_req.blur)
    cfg_non_req = Config("cfg_non_req")
    cfg_empty_default = Config("cfg_empty_default", default_value={})
    cfg_empty_default_parser = Config(
        "cfg_empty_default_parser", default_value="", parser=myparser
    )
    cfg_non_req_parser = Config("cfg_non_req_parser", parser=myparser)

    @timeout(seconds=cfg["timeout"])
    @step
    def start(self):
        print(
            "Non req: %s; emtpy_default %s; empty_default_parser: %s, non_req_parser: %s"
            % (
                self.cfg_non_req,
                self.cfg_empty_default,
                self.cfg_empty_default_parser,
                self.cfg_non_req_parser,
            )
        )
        print("Blur is %s" % self.blur)
        print("Blur2 is %s" % self.blur2)
        print("Config is of type %s" % type(self.cfg))
        self.next(self.end)

    @step
    def end(self):
        print("Blur is %s" % self.blur)
        print("Blur2 is %s" % self.blur2)


if __name__ == "__main__":
    ConfigSimple()
