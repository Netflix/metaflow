from os.path import join
from typing import Dict

from metaflow import FlowSpec, Parameter, resources, s3_sensor, step

"""
This test flow ensures that @s3_sensor properly waits for path to be written
to in S3. In particular, this test ensures `path_formatter` works and users are
able to format their S3 paths with runtime parameters. The test also tests the usage
of @s3_sensor with volumes and shared volumes while using --notify.
See https://zbrt.atl.zillow.net/browse/AIP-5283 - Fixing @s3_sensor usage
with @resources(volume=...) and --notify.
"""


def formatter(path: str, flow_parameters: Dict[str, str]) -> str:
    import os

    return path.format(
        datastore=os.environ["METAFLOW_DATASTORE_SYSROOT_S3"],
        file_name_for_formatter_test=flow_parameters["file_name_for_formatter_test"],
    )


@s3_sensor(
    path=join(
        "{datastore}", "s3_sensor_test_key_files", "{file_name_for_formatter_test}"
    ),
    timeout_seconds=600,
    polling_interval_seconds=5,
    path_formatter=formatter,
)
class S3SensorWithFormatterFlow(FlowSpec):
    file_name_for_formatter_test = Parameter("file_name_for_formatter_test")

    @resources(volume="1G")
    @step
    def start(self):
        print("S3SensorWithFormatterFlow is starting.")
        self.next(self.end)

    @step
    def end(self):
        print("S3SensorWithFormatterFlow is all done.")


if __name__ == "__main__":
    S3SensorWithFormatterFlow()
