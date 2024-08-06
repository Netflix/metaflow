import hashlib
import json
import os

from concurrent.futures import ThreadPoolExecutor
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    _USE_BAKERY,
    FAST_BAKERY_TYPE,
    FAST_BAKERY_AUTH,
    FAST_BAKERY_URL,
    FAST_BAKERY_ENV_PATH,
    get_pinned_conda_libs,
)
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.plugins.pypi.conda_environment import CondaEnvironment
from .fast_bakery import FastBakery, FastBakeryException
from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator
from metaflow.plugins.pypi.pypi_decorator import PyPIStepDecorator

BAKERY_METAFILE = ".imagebakery-cache"

import json
import os
import fcntl
from functools import wraps
from concurrent.futures import ThreadPoolExecutor


def cache_request(cache_file):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            call_args = kwargs.copy()
            call_args.update(zip(func.__code__.co_varnames, args))
            call_args.pop("self", None)
            cache_key = hashlib.md5(
                json.dumps(call_args, sort_keys=True).encode("utf-8")
            ).hexdigest()

            try:
                with open(cache_file, "r") as f:
                    cache = json.load(f)
                    if cache_key in cache:
                        return cache[cache_key]
            except (FileNotFoundError, json.JSONDecodeError):
                cache = {}

            result = func(*args, **kwargs)

            try:
                with open(cache_file, "r+") as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    try:
                        f.seek(0)
                        cache = json.load(f)
                    except json.JSONDecodeError:
                        cache = {}

                    cache[cache_key] = result

                    f.seek(0)
                    f.truncate()
                    json.dump(cache, f)
            except FileNotFoundError:
                with open(cache_file, "w") as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                    json.dump({cache_key: result}, f)

            return result

        return wrapper

    return decorator


class DockerEnvironmentException(MetaflowException):
    headline = "Ran into an error while setting up the environment"

    def __init__(self, msg):
        super(DockerEnvironmentException, self).__init__(msg)


class DockerEnvironment(MetaflowEnvironment):
    TYPE = "fast-bakery"
    _filecache = None

    def __init__(self, flow):
        self.skipped_steps = set()
        self.flow = flow

        self.bakery = FastBakery(url=FAST_BAKERY_URL)

    def set_local_root(self, local_root):
        self.local_root = local_root

    def decospecs(self):
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

    def init_environment(self, echo):
        self.skipped_steps = {
            step.name
            for step in self.flow
            if not any(
                isinstance(deco, (BatchDecorator, KubernetesDecorator))
                for deco in step.decorators
            )
        }

        echo("Baking Docker images for environment(s) ...")

        with ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    self.bake_image_for_step,
                    [step for step in self.flow if step.name not in self.skipped_steps],
                )
            )

        echo("Environments are ready!")
        if self.skipped_steps:
            self.delegate = CondaEnvironment(self.flow)
            self.delegate.set_local_root(self.local_root)
            self.delegate.validate_environment(echo, self.datastore_type)
            self.delegate.init_environment(echo, self.skipped_steps)

    def details_from_step(self, step):
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
        dependency_deco = pypi_deco if pypi_deco is not None else conda_deco
        if dependency_deco is not None:
            python = dependency_deco.attributes["python"]
            pkgs = get_pinned_conda_libs(python, self.datastore_type)
            pkgs.update(dependency_deco.attributes["packages"])
            # request only Conda OR PyPI packages
            if dependency_deco.name == "pypi":
                pypi_pkg = pkgs
                conda_pkg = None
            else:
                conda_pkg = pkgs
                pypi_pkg = None

        return base_image, python, pypi_pkg, conda_pkg, dependency_deco.name

    @cache_request(BAKERY_METAFILE)
    def _cached_bake(self, python, pypi_pkg, conda_pkg, base_image, image_kind):
        self.bakery._reset_payload()
        self.bakery.python_version(python)
        if pypi_pkg:
            self.bakery.pypi_packages(pypi_pkg)
        if conda_pkg:
            self.bakery.conda_packages(conda_pkg)
        if base_image:
            self.bakery.base_image(base_image)
        self.bakery.image_kind(image_kind)

        image = self.bakery.bake()
        return image

    def bake_image_for_step(self, step):

        base_image, python, pypi_pkg, conda_pkg, _deco_type = self.details_from_step(
            step
        )

        try:
            image = self._cached_bake(
                python, pypi_pkg, conda_pkg, base_image, FAST_BAKERY_TYPE
            )
            # We don't have access to the request payload anymore, so we'll create a simplified version
            for deco in step.decorators:
                if _is_remote_deco(deco):
                    deco.attributes["image"] = image["success"]["containerImage"]
        except FastBakeryException as ex:
            raise DockerEnvironmentException(str(ex))

        return step, image

    def executable(self, step_name, default=None):
        if step_name in self.skipped_steps:
            return self.delegate.executable(step_name, default)
        return os.path.join(FAST_BAKERY_ENV_PATH, "bin/python")

    def interpreter(self, step_name):
        if step_name in self.skipped_steps:
            return self.delegate.interpreter(step_name)
        return os.path.join(FAST_BAKERY_ENV_PATH, "bin/python")

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
        if step_name in self.skipped_steps:
            return self.delegate.bootstrap_commands(step_name, datastore_type)
        # Bootstrap conda and execution environment for step
        # we use an internal boolean flag so we do not have to pass the fast bakery endpoint url
        # in order to denote that a bakery has been configured.
        return [
            "export USE_BAKERY=1",
        ] + super().bootstrap_commands(step_name, datastore_type)


def _is_remote_deco(deco):
    return isinstance(deco, BatchDecorator) or isinstance(deco, KubernetesDecorator)
