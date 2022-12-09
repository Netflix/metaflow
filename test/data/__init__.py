import os

# Can set a default path here. Note that you can update the path
# if you want a fresh set of data
S3ROOT = os.environ.get("METAFLOW_S3_TEST_ROOT")

from metaflow.plugins.datatools.s3.s3util import get_s3_client

s3client, _ = get_s3_client()

from metaflow import FlowSpec


# ast parsing in metaflow.graph doesn't like this class
# to be defined in test_s3.py. Defining it here works.
class FakeFlow(FlowSpec):
    def __init__(self, name="FakeFlow", use_cli=False):
        self.name = name


DO_TEST_RUN = False
