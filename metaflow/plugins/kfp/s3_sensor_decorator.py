from types import FunctionType
from typing import Tuple
from urllib.parse import urlparse

from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

"""
Within identity_formatter, which is passed in as the path_formatter parameter,
customers have access to all variables in flow_parameters_json (which 
include all parameters passed in through the flow at compile and run time by
Argo) as well as a number of environment variables.

Please see metaflow/plugin/environment_decorator.py for details on how
to add environment variables to be accessible within steps.

@s3_sensor is implemented as a flow decorator that's used by customers to ensure 
a workflow only begins after a key in S3 has been written to. 

Example usage:

    Note: In the below examples, flow_paramaters = 
    {
        "flow_id": "random_flow_id",
        "date": "07-02-2021"
    }

    If flow_id is a parameter you've passed to your flow, the substitution
    of flow_id in the `path` variable is automatically done for you if you
    specify the variable in braces ({}) (e.g. {flow_id}) AND IF you don't provide
    your own path formatter and don't pass in env variables.

    @s3_sensor(
        path="s3://aip-example-sandbox/metaflow/S3SensorFlow/data/61/flow_id={flow_id}",
        timeout_seconds=3600, # 1 hour
        polling_interval_seconds=90,
    )
    class S3SensorFlow(FlowSpec):    
        ...

    If you want to format flow_id (or any other variable in flow_parameters), you
    can do so with a separate `path_formatter` function.

    def formatter(path: str, flow_parameters: dict) -> str:
        path.format(flow_id=1234, year=flow_parameters["date"].split("-")[-1])

    @s3_sensor(
        path="s3://aip-example-sandbox/metaflow/S3SensorFlow/data/61/{flow_id}/year={year}",
        timeout_seconds=3600, # 1 hour
        polling_interval_seconds=90,
        path_formatter=formatter
    )
    class S3SensorFlow(FlowSpec):    
        ...

    In the above example, we needed to provide `flow_id` in the formatter function because
    we passed in a custom path_formatter function.

    If you plan to use env variables present in the pods on Kubeflow, use os_expandvars=True.
    Note: os_expandvars=False by default.

    @s3_sensor(
        path=join("$METAFLOW_DATASTORE_SYSROOT_S3", "/path/to/file"),
        os_expandvars=True
    )
    class S3SensorFlow(FlowSpec):    
        ...
"""


class S3SensorDecorator(FlowDecorator):
    name = "s3_sensor"
    defaults = {
        "path": "",
        "timeout_seconds": 3600,
        "polling_interval_seconds": 300,
        "path_formatter": None,
        "os_expandvars": False,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.path = self.attributes["path"]
        self.timeout_seconds = self.attributes["timeout_seconds"]
        self.polling_interval_seconds = self.attributes["polling_interval_seconds"]
        self.path_formatter = self.attributes["path_formatter"]
        self.os_expandvars = self.attributes["os_expandvars"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")

        # path_formatter must not be provided and os_expandvars must be false
        # for us to perform this compilation check.
        if not self.path_formatter and not self.os_expandvars:
            parsed_path = urlparse(self.path)
            if not parsed_path.scheme or parsed_path.scheme not in ["s3", "s3a", "s3n"]:
                raise MetaflowException(
                    "Your S3 path must be prefixed by s3://, s3a://, or s3n://"
                )

            bucket, key = parsed_path.netloc, parsed_path.path.lstrip("/")
            if not bucket or not key:
                raise MetaflowException(
                    "Your S3 path must have a nonempty bucket and key."
                )

        # if the user provides a path_formatter, it must be a function
        if self.path_formatter and not isinstance(self.path_formatter, FunctionType):
            raise MetaflowException("path_formatter must be a function.")
