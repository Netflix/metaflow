# pyright: strict, reportTypeCommentUsage=false

from itertools import chain
import json
import os
import time
import tarfile

from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)

from metaflow.datastore.local_storage import LocalStorage
from metaflow.debug import debug
from metaflow.flowspec import FlowSpec

from metaflow.metaflow_config import CONDA_MAGIC_FILE
from metaflow.metaflow_environment import MetaflowEnvironment

from .utils import arch_id, get_conda_manifest_path

from .env_descr import EnvID, ResolvedEnvironment
from .conda import Conda
from .conda_step_decorator import CondaStepDecorator


class CondaEnvironment(MetaflowEnvironment):
    TYPE = "conda"
    _filecache = None
    _conda = None

    def __init__(self, flow: FlowSpec):
        self._flow = flow

        self._conda = None  # type: Optional[Conda]

        # key: EnvID; value: dict containing:
        #  - "id": key
        #  - "steps": steps using this environment
        #  - "arch": architecture of the environment
        #  - "deps": array of requested dependencies
        #  - "channels": additional channels to search
        #  - "resolved": ResolvedEnvironment or None
        #  - "already_resolved": T/F
        self._requested_envs = {}  # type: Dict[EnvID, Dict[str, Any]]

        # A conda environment sits on top of whatever default environment
        # the user has so we get that environment to be able to forward
        # any calls we don't handle specifically to that one.
        from ...plugins import ENVIRONMENTS
        from metaflow.metaflow_config import DEFAULT_ENVIRONMENT

        if DEFAULT_ENVIRONMENT == self.TYPE:
            # If the default environment is Conda itself then fallback on
            # the default 'default environment'
            self.base_env = MetaflowEnvironment(self._flow)
        else:
            self.base_env = [
                e
                for e in ENVIRONMENTS + [MetaflowEnvironment]
                if e.TYPE == DEFAULT_ENVIRONMENT
            ][0](self._flow)

    def init_environment(self, echo: Callable[..., None]):
        # Print a message for now
        echo("Bootstrapping Conda environment... (this could take a few minutes)")

        self._conda = cast(Conda, self._conda)
        for step in self._flow:
            # Figure out the environments that we need to resolve for all steps
            # We will resolve all unique environments in parallel
            step_conda_dec = self._get_conda_decorator(step.__name__)
            env_ids = step_conda_dec.env_ids
            for env_id in env_ids:
                resolved_env = self._conda.environment(env_id)
                if env_id not in self._requested_envs:
                    self._requested_envs[env_id] = {
                        "id": env_id,
                        "steps": [step.name],
                        "deps": step_conda_dec.step_deps,
                        "channels": step_conda_dec.channel_deps,
                        "format": [step_conda_dec.package_format],
                        "resolved": resolved_env,
                        "already_resolved": resolved_env is not None,
                    }
                else:
                    self._requested_envs[env_id]["steps"].append(step.name)
                    if (
                        step_conda_dec.package_format
                        not in self._requested_envs[env_id]["format"]
                    ):
                        self._requested_envs[env_id]["format"].append(
                            step_conda_dec.package_format
                        )

        # At this point, we check in our backend storage if we have the files we need

        need_resolution = [
            env_id
            for env_id, req in self._requested_envs.items()
            if req["resolved"] is None
        ]
        if debug.conda:
            debug.conda_exec("Resolving environments:")
            for env_id in need_resolution:
                info = self._requested_envs[env_id]
                debug.conda_exec(
                    "%s (%s): %s" % (env_id.req_id, env_id.full_id, str(info))
                )
        if len(need_resolution):
            self._resolve_environments(echo, need_resolution)

        if self._datastore_type != "local":
            # We may need to update caches
            # Note that it is possible that something we needed to resolve, we don't need
            # to cache (if we resolved to something already cached).
            update_envs = [
                request["resolved"]
                for request in self._requested_envs.values()
                if not request["already_resolved"]
                or not request["resolved"].is_cached(request["format"])
            ]
            formats = list(
                set(
                    chain(
                        *[
                            request["format"]
                            for request in self._requested_envs.values()
                        ]
                    )
                )
            )

            self._conda.cache_environments(update_envs, formats)
        else:
            update_envs = [
                request["resolved"]
                for request in self._requested_envs.values()
                if not request["already_resolved"]
            ]
        self._conda.add_environments(update_envs)

        # Update the default environment
        for env_id, request in self._requested_envs.items():
            if env_id.full_id == "_default":
                self._conda.set_default_environment(request["resolved"].env_id)

        # We are done -- write back out the environments.
        # TODO: Not great that this is manual
        self._conda.write_out_environments()

        # Delegate to whatever the base environment needs to do.
        self.base_env.init_environment(echo)

    def validate_environment(
        self, echo: Callable[..., Any], datastore_type: str
    ) -> None:
        self._local_root = cast(str, LocalStorage.get_datastore_root_from_config(echo))
        self._datastore_type = datastore_type
        self._conda = Conda(echo, datastore_type)

        return self.base_env.validate_environment(echo, datastore_type)

    def decospecs(self) -> Tuple[str, ...]:
        # Apply conda decorator and base environment's decorators to all steps
        return ("conda",) + self.base_env.decospecs()

    def _resolve_environments(
        self, echo: Callable[..., None], env_ids: Sequence[EnvID]
    ):
        start = time.time()
        if len(env_ids) == len(self._requested_envs):
            echo("    Resolving %d environments in flow ..." % len(env_ids), nl=False)
        else:
            echo(
                "    Resolving %d of %d environments in flows (others are cached) ..."
                % (len(env_ids), len(self._requested_envs)),
                nl=False,
            )

        def _resolve(env_desc: Mapping[str, Any]) -> Tuple[EnvID, ResolvedEnvironment]:
            self._conda = cast(Conda, self._conda)
            env_id = cast(EnvID, env_desc["id"])
            return (
                env_id,
                self._conda.resolve(
                    env_desc["steps"],
                    env_desc["deps"],
                    env_desc["channels"],
                    env_id.arch,
                ),
            )

        self._conda = cast(Conda, self._conda)
        # NOTE: Co-resolved environments allow you to resolve a bunch of "equivalent"
        # environments for different platforms. This is great as it can allow you to
        # run code on Linux and then instantiate an environment to look at it on Mac.
        # One issue though is that the set of packages on Linux may change while those
        # on mac may not (or vice versa) so it is possible to get in the following
        # situation:
        # - Co-resolve at time A:
        #   - Get linux full_id 123 and mac full_id 456
        # - Co-resolve later at time B:
        #   - Get linux full_id 123 and mac full_id 789
        # This is a problem because now the 1:1 correspondence between co-resolved
        # environments (important for figuring out which environment to use) is broken
        #
        # To solve this problem, we consider that co-resolved environments participate
        # in the computation of the full_id (basically a concatenation of all packages
        # across all co-resolved environments). This maintains the 1:1 correspondence.
        # It has a side benefit that we can use that same full_id for all co-resolved
        # environment making one easier to find from the other (instead of using indirect
        # links)
        co_resolved_envs = (
            {}
        )  # type: Dict[str, List[Tuple[EnvID, ResolvedEnvironment]]]
        if len(env_ids):
            with ThreadPoolExecutor() as executor:
                resolution_result = [
                    executor.submit(_resolve, v)
                    for k, v in self._requested_envs.items()
                    if k in env_ids
                ]
                for f in as_completed(resolution_result):
                    env_id, resolved_env = f.result()
                    # This checks if there is the same resolved environment already
                    # cached (in which case, we don't have to check a bunch of things
                    # so makes it nicer)
                    co_resolved_envs.setdefault(env_id.req_id, []).append(
                        (env_id, resolved_env)
                    )

            # Now we know all the co-resolved environments so we can compute the full
            # ID for all those environments
            for envs in co_resolved_envs.values():
                if len(envs) > 1:
                    ResolvedEnvironment.set_coresolved_full_id([x[1] for x in envs])

                for orig_env_id, resolved_env in envs:
                    resolved_env_id = resolved_env.env_id
                    cached_resolved_env = self._conda.environment(resolved_env_id)
                    if cached_resolved_env:
                        resolved_env = cached_resolved_env
                        self._requested_envs[orig_env_id]["already_resolved"] = True

                    self._requested_envs[orig_env_id]["resolved"] = resolved_env
                    debug.conda_exec(
                        "For environment %s (%s) (deps: %s), need packages %s"
                        % (
                            orig_env_id.req_id,
                            orig_env_id.full_id,
                            resolved_env.deps,
                            ", ".join([p.filename for p in resolved_env.packages]),
                        )
                    )

        duration = int(time.time() - start)
        echo(" done in %d seconds." % duration)

    def _get_conda_decorator(self, step_name: str) -> CondaStepDecorator:
        step = next(step for step in self._flow if step.name == step_name)
        decorator = next(
            deco for deco in step.decorators if isinstance(deco, CondaStepDecorator)
        )
        # Guaranteed to have a conda decorator because of self.decospecs()
        return decorator

    def _get_env_id(self, step_name: str) -> Optional[EnvID]:
        conda_decorator = self._get_conda_decorator(step_name)
        if conda_decorator.is_enabled():
            resolved_env = cast(Conda, self._conda).environment(conda_decorator.env_id)
            if resolved_env:
                return resolved_env.env_id
        return None

    def _get_executable(self, step_name: str) -> Optional[str]:
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            # The create method in Conda() sets up this symlink when creating the
            # environment.
            return os.path.join(".", "__conda_python")
        return None

    def bootstrap_commands(self, step_name: str, datastore_type: str) -> List[str]:
        # Bootstrap conda and execution environment for step
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            return [
                "export CONDA_START=$(date +%s)",
                "echo 'Bootstrapping environment ...'",
                'python -m metaflow.plugins.conda.remote_bootstrap "%s" "%s" %s %s %s'
                % (
                    self._flow.name,
                    step_name,
                    env_id.req_id,
                    env_id.full_id,
                    datastore_type,
                ),
                "export _METAFLOW_CONDA_ENV='%s'"
                % json.dumps(env_id).replace('"', '\\"'),
                "echo 'Environment bootstrapped.'",
                "export CONDA_END=$(date +%s)",
            ]
        return []

    def add_to_package(self) -> List[Tuple[str, str]]:
        # TODO: Improve this to only extract the environments that we care about
        # The issue is that we need to pass a path which is a bit annoying since
        # we then can't clean it up easily.
        files = self.base_env.add_to_package()
        # Add conda manifest file to job package at the top level.
        path = get_conda_manifest_path(cast(str, self._local_root))
        if os.path.exists(path):
            files.append((path, os.path.basename(path)))
        return files

    def pylint_config(self) -> List[str]:
        config = self.base_env.pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def executable(self, step_name: str) -> str:
        # Get relevant python interpreter for step
        executable = self._get_executable(step_name)
        if executable is not None:
            return executable
        return self.base_env.executable(step_name)

    @classmethod
    def get_client_info(
        cls, flow_name: str, metadata: Dict[str, str]
    ) -> Dict[str, Any]:
        # TODO: FIX THIS: this doesn't work with the new format.
        if cls._filecache is None:
            from metaflow.client.filecache import FileCache

            cls._filecache = FileCache()
        info = metadata.get("code-package")
        env_id = metadata.get("conda_env_id")
        if info is None or env_id is None:
            return {"type": "conda"}
        info = json.loads(info)
        _, blobdata = cls._filecache.get_data(
            info["ds_type"], flow_name, info["location"], info["sha"]
        )
        with tarfile.open(fileobj=BytesIO(blobdata), mode="r:gz") as tar:
            conda_file = tar.extractfile(CONDA_MAGIC_FILE)
        if conda_file is None:
            return {"type": "conda"}
        info = json.loads(conda_file.read().decode("utf-8"))
        new_info = {
            "type": "conda",
            "explicit": info[env_id]["explicit"],
            "deps": info[env_id]["deps"],
        }
        return new_info

    def get_package_commands(self, code_package_url: str, datastore_type: str):
        return self.base_env.get_package_commands(code_package_url, datastore_type)

    def get_environment_info(self):
        return self.base_env.get_environment_info()
