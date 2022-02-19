from .exceptions import AirflowNotPresent
import rich

AIRFLOW_PREFIX = "arf"


class Airflow(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package_sha,
        code_package_url,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        is_project=False,
    ):
        try:
            import airflow
        except ImportError:
            raise AirflowNotPresent
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers

    def compile(self):
        rich.inspect(self)

    def _make_dag(self):
        pass
