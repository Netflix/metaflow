# pyright: strict, reportTypeCommentUsage=false

import importlib
import json
import os
import platform
import shutil
import sys
import tempfile

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)

from metaflow.datastore import LocalStorage
from metaflow.datastore.flow_datastore import FlowDataStore
from metaflow.datastore.task_datastore import TaskDataStore
from metaflow.decorators import Decorator, StepDecorator
from metaflow.extension_support import EXT_PKG
from metaflow.flowspec import FlowSpec
from metaflow.graph import FlowGraph
from metaflow.metadata import MetaDatum
from metaflow.metadata.metadata import MetadataProvider
from metaflow.metaflow_config import (
    CONDA_REMOTE_COMMANDS,
    CONDA_PREFERRED_FORMAT,
    get_pinned_conda_libs,
)
from metaflow.metaflow_environment import (
    InvalidEnvironmentException,
    MetaflowEnvironment,
)
from metaflow.plugins.conda.env_descr import (
    CachedEnvironmentInfo,
    EnvID,
    ResolvedEnvironment,
)
from metaflow.plugins.env_escape import generate_trampolines
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.util import get_metaflow_root

from .utils import arch_id
from .env_descr import read_conda_manifest
from .conda import Conda


class FooDecorator(StepDecorator):
    name = "foo"
    defaults = {
        "some_dict": {
            "an_int": 123,
            "a_float": 456.7,
            "a_string": "hello",
            "a list": [1, "a"],
            "a dict": {"a": 1},
        },
        "some_list": [123, "hello world", "hi: you", "hello there, how are you"],
        "some_string": "hello, world: I am well",
        "some_none": None,
        "some_magic": None,
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(FooDecorator, self).__init__(attributes, False)
        print("REC: IN FOODECORATOR with %s" % str(self.attributes))


class CondaStepDecorator(StepDecorator):
    """
    Specifies the Conda environment for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base` flow-level decorator. Hence
    you can use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    libraries : Dict
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`).
    python : string
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Conda (default: False).
    """

    name = "conda"
    defaults = {
        "libraries": {},
        "channels": [],
        "archs": None,
        "python": None,
        "disabled": None,
        "package_format": None,
    }  # type: Dict[str, Any]

    conda = None  # type: Optional[Conda]
    _local_root = None  # type: Optional[str]

    def is_enabled(self, ubf_context: Optional[str] = None) -> bool:
        if ubf_context == UBF_CONTROL:
            return False
        return not next(
            x
            for x in [
                self.attributes["disabled"],
                self._base_attributes["disabled"],
                False,
            ]
            if x is not None
        )

    @property
    def env_ids(self) -> List[EnvID]:
        return [
            EnvID(
                req_id=ResolvedEnvironment.get_req_id(
                    self.step_deps, self.channel_deps
                ),
                full_id="_default",
                arch=arch,
            )
            for arch in self.requested_architectures
        ]

    @property
    def env_id(self) -> EnvID:
        # This is always for "where we need to execute locally" because the
        # local arch (or remote one needed if applicable) is always first
        return self.env_ids[0]

    @property
    def channel_deps(self) -> Sequence[str]:
        channels = list(self.attributes["channels"])  # type: List[str]
        channels.extend(self._base_attributes["channels"])

        return channels

    @property
    def step_deps(self) -> Sequence[str]:
        deps = ["python==%s" % self._python_version()]
        deps.extend("%s==%s" % (name, ver) for name, ver in self._lib_deps().items())
        return deps

    @property
    def requested_architectures(self) -> List[str]:
        return self._archs

    @property
    def package_format(self) -> str:
        return next(
            x
            for x in [
                self.attributes["package_format"],
                self._base_attributes["package_format"],
                CONDA_PREFERRED_FORMAT,
            ]
            if x is not None
        )

    @property
    def local_root(self) -> Optional[str]:
        return self._local_root

    def step_init(
        self,
        flow: FlowSpec,
        graph: FlowGraph,
        step_name: str,
        decorators: Sequence[StepDecorator],
        environment: MetaflowEnvironment,
        flow_datastore: FlowDataStore,
        logger: Callable[..., None],
    ):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@conda* decorator requires " "--environment=conda"
            )

        self._echo = logger
        self._env = environment
        self._flow = flow
        self._step_name = step_name
        self._flow_datastore_type = flow_datastore.TYPE  # type: str
        self._base_attributes = self._get_base_attributes()

        self._archs = self._architectures(decorators)

        self.__class__._local_root = LocalStorage.get_datastore_root_from_config(
            self._echo
        )  # type: str
        os.environ["PYTHONNOUSERSITE"] = "1"

    def runtime_init(self, flow: FlowSpec, graph: FlowGraph, package: Any, run_id: str):
        # Create a symlink to installed version of metaflow to execute user code against
        path_to_metaflow = os.path.join(get_metaflow_root(), "metaflow")
        path_to_info = os.path.join(get_metaflow_root(), "INFO")
        self._metaflow_home = tempfile.mkdtemp(dir="/tmp")
        self._addl_paths = None
        os.symlink(path_to_metaflow, os.path.join(self._metaflow_home, "metaflow"))

        # Symlink the INFO file as well to properly propagate down the Metaflow version
        # if launching on AWS Batch for example
        if os.path.isfile(path_to_info):
            os.symlink(path_to_info, os.path.join(self._metaflow_home, "INFO"))
        else:
            # If there is no "INFO" file, we will actually create one in this new
            # place because we won't be able to properly resolve the EXT_PKG extensions
            # the same way as outside conda (looking at distributions, etc). In a
            # Conda environment, as shown below (where we set self.addl_paths), all
            # EXT_PKG extensions are PYTHONPATH extensions. Instead of re-resolving,
            # we use the resolved information that is written out to the INFO file.
            with open(
                os.path.join(self._metaflow_home, "INFO"), mode="wt", encoding="utf-8"
            ) as f:
                f.write(json.dumps(self._env.get_environment_info()))

        # Do the same for EXT_PKG
        try:
            m = importlib.import_module(EXT_PKG)
        except ImportError:
            # No additional check needed because if we are here, we already checked
            # for other issues when loading at the toplevel
            pass
        else:
            custom_paths = list(set(m.__path__))  # For some reason, at times, unique
            # paths appear multiple times. We simplify
            # to avoid un-necessary links

            if len(custom_paths) == 1:
                # Regular package; we take a quick shortcut here
                os.symlink(
                    custom_paths[0],
                    os.path.join(self._metaflow_home, EXT_PKG),
                )
            else:
                # This is a namespace package, we therefore create a bunch of directories
                # so we can symlink in those separately and we will add those paths
                # to the PYTHONPATH for the interpreter. Note that we don't symlink
                # to the parent of the package because that could end up including
                # more stuff we don't want
                self.addl_paths = []  # type: List[str]
                for p in custom_paths:
                    temp_dir = tempfile.mkdtemp(dir=self._metaflow_home)
                    os.symlink(p, os.path.join(temp_dir, EXT_PKG))
                    self.addl_paths.append(temp_dir)

        # Also install any environment escape overrides directly here to enable
        # the escape to work even in non metaflow-created subprocesses
        generate_trampolines(self._metaflow_home)

    def runtime_step_cli(
        self,
        cli_args: Any,  # Importing CLIArgs causes an issue so ignore for now
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: str,
    ):

        no_remote = all([x not in cli_args.commands for x in CONDA_REMOTE_COMMANDS])
        if self.is_enabled(ubf_context) and no_remote:
            self._get_conda(self._echo, self._flow_datastore_type)
            assert self.conda
            resolved_env = cast(
                ResolvedEnvironment, self.conda.environment(self.env_id)
            )
            my_env_id = resolved_env.env_id
            # Export this for local runs, we will use it to read the "resolved"
            # environment ID in task_pre_step; this makes it compatible with the remote
            # bootstrap which also exports it.
            cli_args.env["_METAFLOW_CONDA_ENV"] = json.dumps(my_env_id)
            # Create the environment we are going to use
            if self.conda.created_environment(my_env_id):
                self._echo(
                    "Using existing Conda environment %s (%s)"
                    % (my_env_id.req_id, my_env_id.full_id)
                )
            else:
                # Otherwise, we read the conda file and create the environment locally
                self._echo(
                    "Creating Conda environment %s (%s)..."
                    % (my_env_id.req_id, my_env_id.full_id)
                )
                self.conda.create_for_step(self._step_name, resolved_env)

            # Actually set it up.
            python_path = self._metaflow_home
            if self.addl_paths is not None:
                addl_paths = os.pathsep.join(self.addl_paths)
                python_path = os.pathsep.join([addl_paths, python_path])

            cli_args.env["PYTHONPATH"] = python_path
            cli_args.entrypoint[0] = self.conda.python(my_env_id)

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: TaskDataStore,
        meta: MetadataProvider,
        run_id: str,
        task_id: str,
        flow: FlowSpec,
        graph: FlowGraph,
        retry_count: int,
        max_retries: int,
        ubf_context: str,
        inputs: List[str],
    ):
        if self.is_enabled(ubf_context):
            # Add the Python interpreter's parent to the path. This is to
            # ensure that any non-pythonic dependencies introduced by the conda
            # environment are visible to the user code.
            env_path = os.path.dirname(os.path.realpath(sys.executable))
            if os.environ.get("PATH") is not None:
                env_path = os.pathsep.join([env_path, os.environ["PATH"]])
            os.environ["PATH"] = env_path

            meta.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field="conda_env_id",
                        value=os.environ["_METAFLOW_CONDA_ENV"],
                        type="conda_env_id",
                        tags=["attempt_id:{0}".format(retry_count)],
                    )
                ],
            )

    def runtime_finished(self, exception: Exception):
        shutil.rmtree(self._metaflow_home)

    def _get_base_attributes(self) -> Dict[str, Any]:
        if "conda_base" in self._flow._flow_decorators:
            return self._flow._flow_decorators["conda_base"].attributes
        return self.defaults

    def _python_version(self) -> str:
        return next(
            x
            for x in [
                self.attributes["python"],
                self._base_attributes["python"],
                platform.python_version(),
            ]
            if x is not None
        )

    def _lib_deps(self) -> Dict[str, str]:
        deps = get_pinned_conda_libs(self._python_version(), self._flow_datastore_type)

        deps.update(self._base_attributes["libraries"])
        deps.update(self.attributes["libraries"])

        return deps

    def _architectures(self, decos: Iterable[Decorator]) -> List[str]:
        archs = [arch_id()]
        for deco in decos:
            if deco.name in CONDA_REMOTE_COMMANDS:
                # force conda resolution for linux-64 architectures
                archs = ["linux-64"]
        archs.extend(
            next(
                x
                for x in [self.attributes["archs"], self._base_attributes["archs"], []]
                if x is not None
            )
        )
        return archs

    @classmethod
    def _get_conda(cls, echo: Callable[..., None], datastore_type: str) -> None:
        if cls.conda is None:
            cls.conda = Conda(echo, datastore_type)
