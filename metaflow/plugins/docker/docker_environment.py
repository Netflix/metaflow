import os

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    _USE_BAKERY,
)
from metaflow.metaflow_environment import MetaflowEnvironment
from .bakery import bake_image
from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator


class DockerEnvironmentException(MetaflowException):
    headline = "Ran into an error while setting up the environment"

    def __init__(self, msg):
        super(DockerEnvironmentException, self).__init__(msg)


class DockerEnvironment(MetaflowEnvironment):
    TYPE = "docker"
    _filecache = None

    def __init__(self, flow):
        self.flow = flow

    def set_local_root(self, local_root):
        # TODO: Make life simple by passing echo to the constructor and getting rid of
        # this method's invocation in the decorator
        self.local_root = local_root

    def decospecs(self):
        # Apply conda decorator to manage the task execution lifecycle.
        return ("conda",) + super().decospecs()

    def validate_environment(self, echo, datastore_type):
        self.datastore_type = datastore_type
        self.echo = echo

        # Avoiding circular imports.
        from metaflow.plugins import DATASTORES

        self.datastore = [d for d in DATASTORES if d.TYPE == self.datastore_type][0]

        # Use remote image bakery for conda environments if configured.
        if not _USE_BAKERY:
            raise DockerEnvironmentException("Image Bakery is not configured.")

    def init_environment(self, echo):
        # First resolve environments through Conda, before PyPI.
        echo("Baking Docker images for environment(s) ...")
        # do the magic
        for step in self.flow:
            self.bake_image_for_step(step)
        echo("Environments are ready!")

    def bake_image_for_step(self, step):
        def _is_remote_deco(deco):
            return isinstance(deco, BatchDecorator) or isinstance(
                deco, KubernetesDecorator
            )

        # At the moment we only support ImageBakery for fully remotely executing flows
        if not any(_is_remote_deco(deco) for deco in step.decorators):
            raise DockerEnvironmentException(
                "Docker images are not supported for local steps. "
                "Step **%s** is not set too execute remotely" % step.name
            )
        # map out if user is requesting a base image to build on top of
        base_image = None
        for deco in step.decorators:
            if _is_remote_deco(deco):
                base_image = deco.attributes.get("image", None)

        image = None
        for deco in step.decorators:
            if isinstance(deco, CondaStepDecorator):
                pkgs = deco.attributes["packages"]
                python = deco.attributes["python"]
                image = bake_image(python, pkgs, self.datastore.TYPE, base_image)

        if image is not None:
            # we have an image that we need to set to a kubernetes or batch decorator.
            for deco in step.decorators:
                if _is_remote_deco(deco):
                    deco.attributes["image"] = image

    def executable(self, step_name, default=None):
        return os.path.join("/conda-prefix", "bin/python")

    def interpreter(self, step_name):
        return os.path.join("/conda-prefix", "bin/python")

    def is_disabled(self, step):
        for decorator in step.decorators:
            # @conda decorator is guaranteed to exist thanks to self.decospecs
            if decorator.name in ["conda", "pypi"]:
                # handle @conda/@pypi(disabled=True)
                disabled = decorator.attributes["disabled"]
                return str(disabled).lower() == "true"
        return False

    def pylint_config(self):
        config = super().pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def bootstrap_commands(self, step_name, datastore_type):
        # Bootstrap conda and execution environment for step
        # we use an internal boolean flag so we do not have to pass the image bakery endpoint url
        # in order to denote that a bakery has been configured.
        return [
            "export USE_BAKERY=1",
        ] + super().bootstrap_commands(step_name, datastore_type)
