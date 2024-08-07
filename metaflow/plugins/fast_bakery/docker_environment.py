import hashlib
import json
import os

from concurrent.futures import ThreadPoolExecutor
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    FAST_BAKERY_URL,
    get_pinned_conda_libs,
)
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.plugins.pypi.conda_environment import CondaEnvironment
from .fast_bakery import FastBakery, FastBakeryException
from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator
from metaflow.plugins.pypi.pypi_decorator import PyPIStepDecorator

# TODO: move under .metaflow
BAKERY_METAFILE = ".imagebakery-cache"

import json
import os
import fcntl
from functools import wraps
from concurrent.futures import ThreadPoolExecutor


# TODO - ensure that both @conda/@pypi are not assigned to the same step


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
        self.results = {}

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

    def init_environment(self, echo):
        self.skipped_steps = {
            step.name
            for step in self.flow
            if not any(
                isinstance(deco, (BatchDecorator, KubernetesDecorator))
                for deco in step.decorators
            )
        }

        steps_to_bake = [
            step for step in self.flow if step.name not in self.skipped_steps
        ]
        if steps_to_bake:
            echo("Baking container image(s) ...")
            self.results = self._bake(steps_to_bake)
            for step in self.flow:
                for d in step.decorators:
                    if isinstance(d, (BatchDecorator, KubernetesDecorator)):
                        d.attributes["image"] = self.results[step.name]["success"][
                            "containerImage"
                        ]
                        d.attributes["executable"] = self.results[step.name]["success"][
                            "pythonPath"
                        ]
            echo("Container image(s) baked!")

        if self.skipped_steps:
            self.delegate = CondaEnvironment(self.flow)
            self.delegate.set_local_root(self.local_root)
            self.delegate.validate_environment(echo, self.datastore_type)
            self.delegate.init_environment(echo, self.skipped_steps)

    def _bake(self, steps):
        @cache_request(BAKERY_METAFILE)
        def _cached_bake(
            python=None, pypi_packages=None, conda_packages=None, base_image=None
        ):
            self.bakery._reset_payload()
            self.bakery.python_version(python)
            self.bakery.pypi_packages(pypi_packages)
            self.bakery.conda_packages(conda_packages)
            self.bakery.base_image(base_image)
            # self.bakery.ignore_cache()
            try:
                return self.bakery.bake()
            except FastBakeryException as ex:
                raise DockerEnvironmentException(str(ex))

        def prepare_step(step):
            base_image = next(
                (
                    d.attributes.get("image")
                    for d in step.decorators
                    if isinstance(d, (KubernetesDecorator))
                ),
                None,
            )
            dependencies = next(
                (
                    d
                    for d in step.decorators
                    if isinstance(d, (CondaStepDecorator, PyPIStepDecorator))
                ),
                None,
            )
            python = next(
                (
                    d.attributes["python"]
                    for d in step.decorators
                    if isinstance(d, CondaStepDecorator)
                ),
                None,
            )
            packages = get_pinned_conda_libs(python, self.datastore_type)
            packages.update(dependencies.attributes["packages"] if dependencies else {})

            return {
                "python": python,
                "pypi_packages": packages
                if isinstance(dependencies, PyPIStepDecorator)
                else None,
                "conda_packages": packages
                if isinstance(dependencies, CondaStepDecorator)
                else None,
                "base_image": base_image,
            }

        with ThreadPoolExecutor() as executor:
            return {
                step.name: _cached_bake(**args)
                for step, args in zip(steps, executor.map(prepare_step, steps))
            }

    def executable(self, step_name, default=None):
        if step_name in self.skipped_steps:
            return self.delegate.executable(step_name, default)
        # default is set to the right executable
        return default

    def interpreter(self, step_name):
        if step_name in self.skipped_steps:
            return self.delegate.interpreter(step_name)
        return None

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
        return super().bootstrap_commands(step_name, datastore_type)
