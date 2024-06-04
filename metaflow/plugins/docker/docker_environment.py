import os

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    _USE_BAKERY,
)
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.plugins.pypi.conda_environment import CondaEnvironment
from .fast_bakery import bake_image
from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator
from metaflow.plugins.pypi.pypi_decorator import PyPIStepDecorator


class DockerEnvironmentException(MetaflowException):
    headline = "Ran into an error while setting up the environment"

    def __init__(self, msg):
        super(DockerEnvironmentException, self).__init__(msg)


class DockerEnvironment(MetaflowEnvironment):
    TYPE = "docker"
    _filecache = None

    def __init__(self, flow):
        self.steps_to_delegate = set()
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

        # Use remote fast bakery for conda environments if configured.
        if not _USE_BAKERY:
            raise DockerEnvironmentException("Fast Bakery is not configured.")

    def _init_conda_fallback(self):
        # TODO: In the future we want to support executing with Docker even locally.
        for step in self.flow:
            if not any(_is_remote_deco(deco) for deco in step.decorators):
                # We need to fall back to the Conda environmment for flows that are trying to execute anything locally.
                self.steps_to_delegate.add(step.name)
        if not self.steps_to_delegate:
            return
        # TODO: move to init if possible.
        self.delegate = CondaEnvironment(self.flow)
        self.delegate.set_local_root(self.local_root)

    def init_environment(self, echo):
        self._init_conda_fallback()
        # First resolve environments through Conda, before PyPI.
        echo("Baking Docker images for environment(s) ...")
        for step in self.flow:
            self.bake_image_for_step(step)
        echo("Environments are ready!")
        if self.steps_to_delegate:
            # TODO: add debug echo to output steps that required a conda environment.
            # The delegated conda environment also need to validate and init.
            # we pass a set of steps we want to init to restrict the conda environments created.
            self.delegate.validate_environment(echo, self.datastore_type)
            self.delegate.init_environment(echo, self.steps_to_delegate)

    def bake_image_for_step(self, step):
        if self._is_delegated(step.name):
            # do not bake images for delegated steps
            return
        # map out if user is requesting a base image to build on top of
        base_image = None
        for deco in step.decorators:
            if _is_remote_deco(deco):
                base_image = deco.attributes.get("image", None)

        conda_deco = next(
            (deco for deco in step.decorators if isinstance(deco, CondaStepDecorator)),
            None,
        )
        pypi_deco = next(
            (deco for deco in step.decorators if isinstance(deco, PyPIStepDecorator)),
            None,
        )
        image = None
        # pypi packages need to take precedence over conda.
        # a conda decorator always exists alongside pypi so this needs to be accounted for
        dependency_deco = pypi_deco if pypi_deco is not None else conda_deco
        if dependency_deco is not None:
            pkgs = dependency_deco.attributes["packages"]
            python = dependency_deco.attributes["python"]
            image = bake_image(
                python, pkgs, self.datastore.TYPE, base_image, dependency_deco.name
            )

        if image is not None:
            # we have an image that we need to set to a kubernetes or batch decorator.
            for deco in step.decorators:
                if _is_remote_deco(deco):
                    deco.attributes["image"] = image

    def _is_delegated(self, step_name):
        return step_name in self.steps_to_delegate

    def executable(self, step_name, default=None):
        if self._is_delegated(step_name):
            return self.delegate.executable(step_name, default)
        return os.path.join("/conda-prefix", "bin/python")

    def interpreter(self, step_name):
        if self._is_delegated(step_name):
            return self.delegate.interpreter(step_name)
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
        if self._is_delegated(step_name):
            return self.delegate.bootstrap_commands(step_name, datastore_type)
        # Bootstrap conda and execution environment for step
        # we use an internal boolean flag so we do not have to pass the fast bakery endpoint url
        # in order to denote that a bakery has been configured.
        return [
            "export USE_BAKERY=1",
        ] + super().bootstrap_commands(step_name, datastore_type)


def _is_remote_deco(deco):
    return isinstance(deco, BatchDecorator) or isinstance(deco, KubernetesDecorator)
