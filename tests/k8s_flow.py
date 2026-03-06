import os

from metaflow import FlowSpec, environment, step


IN_CLUSTER_S3_ENDPOINT = os.getenv(
    "METAFLOW_TEST_IN_CLUSTER_S3_ENDPOINT_URL",
    "http://minio.minio.svc.cluster.local:9000",
)
FLOW_ENV_VARS = {
    "METAFLOW_S3_ENDPOINT_URL": IN_CLUSTER_S3_ENDPOINT,
    # Kubernetes/Argo task pods need explicit object-store credentials.
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
}


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


if __name__ == "__main__":
    K8sFlow()
