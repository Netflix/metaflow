from .snowpark_client import SnowparkClient
from .snowpark_service_spec import (
    Container,
    Resources,
    SnowparkServiceSpec,
    VolumeMount,
)
from .snowpark_exceptions import SnowparkException

mapping = str.maketrans("0123456789", "abcdefghij")


class SnowparkJob(object):
    def __init__(self, client: SnowparkClient, name, command, **kwargs):
        self.client = client
        self.name = name
        self.command = command
        self.kwargs = kwargs
        self.container_name = self.name.translate(mapping).lower()

    def create_job_spec(self):
        if self.kwargs.get("image") is None:
            raise SnowparkException(
                "Unable to launch job on Snowpark Container Services. No docker 'image' specified."
            )

        if self.kwargs.get("stage") is None:
            raise SnowparkException(
                "Unable to launch job on Snowpark Container Services. No 'stage' specified."
            )

        if self.kwargs.get("compute_pool") is None:
            raise SnowparkException(
                "Unable to launch job on Snowpark Container Services. No 'compute_pool' specified."
            )

        resources = Resources(
            requests={
                k: v
                for k, v in [
                    ("cpu", self.kwargs.get("cpu")),
                    ("nvidia.com/gpu", self.kwargs.get("gpu")),
                    ("memory", self.kwargs.get("memory")),
                ]
                if v
            }
        )

        volume_mounts = self.kwargs.get("volume_mounts")
        vm_objs = []
        if volume_mounts:
            if isinstance(volume_mounts, str):
                volume_mounts = [volume_mounts]
            for vm in volume_mounts:
                name, mount_path = vm.split(":", 1)
                vm_objs.append(VolumeMount(name=name, mount_path=mount_path))

        container = (
            Container(name=self.container_name, image=self.kwargs.get("image"))
            .env(self.kwargs.get("environment_variables"))
            .resources(resources)
            .volume_mounts(vm_objs)
            .command(self.command)
        )

        self.spec = SnowparkServiceSpec().containers([container])
        return self

    def environment_variable(self, name, value):
        # Never set to None
        if value is None:
            return self
        self.kwargs["environment_variables"] = dict(
            self.kwargs.get("environment_variables", {}), **{name: value}
        )
        return self

    def create(self):
        return self.create_job_spec()

    def execute(self):
        query_id, service_name = self.client.submit(
            self.name,
            self.spec,
            self.kwargs.get("stage"),
            self.kwargs.get("compute_pool"),
            self.kwargs.get("external_integration"),
        )
        return RunningJob(
            client=self.client, query_id=query_id, service_name=service_name
        )

    def image(self, image):
        self.kwargs["image"] = image
        return self

    def stage(self, stage):
        self.kwargs["stage"] = stage
        return self

    def compute_pool(self, compute_pool):
        self.kwargs["compute_pool"] = compute_pool
        return self

    def volume_mounts(self, volume_mounts):
        self.kwargs["volume_mounts"] = volume_mounts
        return self

    def external_integration(self, external_integration):
        self.kwargs["external_integration"] = external_integration
        return self

    def cpu(self, cpu):
        self.kwargs["cpu"] = cpu
        return self

    def gpu(self, gpu):
        self.kwargs["gpu"] = gpu
        return self

    def memory(self, memory):
        self.kwargs["memory"] = memory
        return self


class RunningJob(object):
    def __init__(self, client, query_id, service_name):
        self.client = client
        self.query_id = query_id
        self.service_name = service_name

        db = self.client.session.get_current_database()
        schema = self.client.session.get_current_schema()

        # wait because 'status' might not be ready, and will give 404
        # TODO: do this in a better way..
        import time

        time.sleep(5)
        self.service = (
            self.client.root.databases[db].schemas[schema].services[self.service_name]
        )

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self.query_id)

    @property
    def id(self):
        return self.query_id

    @property
    def job_name(self):
        return self.service_name

    def status_obj(self):
        from snowflake.core.exceptions import APIError

        try:
            return self.service.get_service_status()
        except APIError as e:
            # TODO: maybe retry, sometimes can happen
            # because of too many concurrent requests
            pass

    @property
    def status(self):
        return self.status_obj()[0].get("status")

    @property
    def message(self):
        return self.status_obj()[0].get("message")

    @property
    def is_waiting(self):
        return self.status == "PENDING"

    @property
    def is_running(self):
        return self.status in ["PENDING", "READY"]

    @property
    def has_failed(self):
        return self.status == "FAILED"

    @property
    def has_succeeded(self):
        return self.status == "DONE"

    @property
    def has_finished(self):
        return self.has_succeeded or self.has_failed

    def kill(self):
        from snowflake.core.exceptions import NotFoundError

        try:
            if not self.has_finished:
                self.client.terminate_job(service=self.service)
        except (NotFoundError, TypeError):
            pass