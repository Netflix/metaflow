import os

from metaflow import FlowSpec, environment, step


IN_CLUSTER_S3_ENDPOINT = os.getenv(
    "METAFLOW_TEST_IN_CLUSTER_S3_ENDPOINT_URL",
    "http://minio.minio.svc.cluster.local:9000",
)
def get_flow_env_vars():
    return {
        "METAFLOW_S3_ENDPOINT_URL": IN_CLUSTER_S3_ENDPOINT,
def get_flow_env_vars():
    return {
        "METAFLOW_S3_ENDPOINT_URL": IN_CLUSTER_S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }

class K8sFlow(FlowSpec):
def get_flow_env_vars():
    return {
        "METAFLOW_S3_ENDPOINT_URL": IN_CLUSTER_S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }

FLOW_ENV_VARS = get_flow_env_vars()


class K8sFlow(FlowSpec):
    @environment(vars=FLOW_ENV_VARS)
    @step
    def start(self):
        self.message = "Metaflow Kubernetes + Argo E2E check"
        print("Starting Kubernetes-backed flow execution")
        self.next(self.end)

    @environment(vars=FLOW_ENV_VARS)
    @step
    def end(self):
        print(self.message)
    @environment(vars=FLOW_ENV_VARS)
    @step
    def end(self):
        print(self.message)


if __name__ == "__main__":
    K8sFlow()
