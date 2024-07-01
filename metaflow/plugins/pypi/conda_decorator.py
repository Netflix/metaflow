import importlib
import json
import os
import platform
import re
import sys
import tempfile

from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.extension_support import EXT_PKG
from metaflow.metadata import MetaDatum
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.util import get_metaflow_root

from ... import INFO_FILE


class CondaStepDecorator(StepDecorator):
    """
    Specifies the Conda environment for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base` flow-level decorator. Hence,
    you can use `@conda_base` to set packages required by all
    steps and use `@conda` to specify step-specific overrides.

    Parameters
    ----------
    packages : Dict[str, str], default {}
        Packages to use for this step. The key is the name of the package
        and the value is the version to use.
    libraries : Dict[str, str], default {}
        Supported for backward compatibility. When used with packages, packages will take precedence.
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. A default value of None implies
        that the version used will correspond to the version of the Python interpreter used to start the run.
    disabled : bool, default False
        If set to True, disables @conda.
    """

    name = "conda"
    defaults = {
        "packages": {},
        "libraries": {},  # Deprecated! Use packages going forward
        "python": None,
        "disabled": None,
    }
    # To define conda channels for the whole solve, users can specify
    # CONDA_CHANNELS in their environment. For pinning specific packages to specific
    # conda channels, users can specify channel::package as the package name.

    def __init__(self, attributes=None, statically_defined=False):
        super(CondaStepDecorator, self).__init__(attributes, statically_defined)

        # Support legacy 'libraries=' attribute for the decorator.
        self.attributes["packages"] = {
            **self.attributes["libraries"],
            **self.attributes["packages"],
        }
        del self.attributes["libraries"]

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # The init_environment hook for Environment creates the relevant virtual
        # environments. The step_init hook sets up the relevant state for that hook to
        # do it's magic.

        self.flow = flow
        self.step = step
        self.environment = environment
        self.datastore = flow_datastore

        # Support flow-level decorator.
        if "conda_base" in self.flow._flow_decorators:
            super_attributes = self.flow._flow_decorators["conda_base"][0].attributes
            self.attributes["packages"] = {
                **super_attributes["packages"],
                **self.attributes["packages"],
            }
            self.attributes["python"] = (
                self.attributes["python"] or super_attributes["python"]
            )
            self.attributes["disabled"] = (
                self.attributes["disabled"]
                if self.attributes["disabled"] is not None
                else super_attributes["disabled"]
            )

        # Set default for `disabled` argument.
        if not self.attributes["disabled"]:
            self.attributes["disabled"] = False
        # Set Python interpreter to user's Python if necessary.
        if not self.attributes["python"]:
            self.attributes["python"] = platform.python_version()  # CPython!

        # @conda uses a conda environment to create a virtual environment.
        # The conda environment can be created through micromamba.
        _supported_virtual_envs = ["conda"]

        # To placate people who don't want to see a shred of conda in UX, we symlink
        # --environment=pypi to --environment=conda
        _supported_virtual_envs.extend(["pypi"])

        # TODO: Hardcoded for now to support Docker environment.
        # We should introduce a more robust mechanism for appending supported environments, for example from within extensions.
        _supported_virtual_envs.extend(["docker"])

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @conda
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    " or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )

        # At this point, the list of 32 bit instance types is shrinking quite rapidly.
        # We can worry about supporting them when there is a need.

        # TODO: This code snippet can be done away with by altering the constructor of
        #       MetaflowEnvironment. A good first-task exercise.
        # Avoid circular import
        from metaflow.plugins.datastores.local_storage import LocalStorage

        environment.set_local_root(LocalStorage.get_datastore_root_from_config(logger))

        self.disabled = self.environment.is_disabled(
            next(step for step in self.flow if step.name == self.step)
        )

    def runtime_init(self, flow, graph, package, run_id):
        if self.disabled:
            return
        # Create a symlink to metaflow installed outside the virtual environment.
        self.metaflow_dir = tempfile.TemporaryDirectory(dir="/tmp")
        os.symlink(
            os.path.join(get_metaflow_root(), "metaflow"),
            os.path.join(self.metaflow_dir.name, "metaflow"),
        )

        info = os.path.join(get_metaflow_root(), os.path.basename(INFO_FILE))
        # Symlink the INFO file as well to properly propagate down the Metaflow version
        if os.path.isfile(info):
            os.symlink(
                info, os.path.join(self.metaflow_dir.name, os.path.basename(INFO_FILE))
            )
        else:
            # If there is no info file, we will actually create one in this new
            # place because we won't be able to properly resolve the EXT_PKG extensions
            # the same way as outside conda (looking at distributions, etc.). In a
            # Conda environment, as shown below (where we set self.addl_paths), all
            # EXT_PKG extensions are PYTHONPATH extensions. Instead of re-resolving,
            # we use the resolved information that is written out to the INFO file.
            with open(
                os.path.join(self.metaflow_dir.name, os.path.basename(INFO_FILE)),
                mode="wt",
                encoding="utf-8",
            ) as f:
                f.write(
                    json.dumps(
                        self.environment.get_environment_info(include_ext_info=True)
                    )
                )

        # Support metaflow extensions.
        self.addl_paths = None
        try:
            m = importlib.import_module(EXT_PKG)
        except ImportError:
            # No additional check needed because if we are here, we already checked
            # for other issues when loading at the toplevel.
            pass
        else:
            custom_paths = list(set(m.__path__))
            # For some reason, at times, unique paths appear multiple times. We
            # simplify to avoid un-necessary links.

            if len(custom_paths) == 1:
                # Regular package; we take a quick shortcut here.
                os.symlink(
                    custom_paths[0],
                    os.path.join(self.metaflow_dir.name, EXT_PKG),
                )
            else:
                # This is a namespace package, we therefore create a bunch of
                # directories so that we can symlink in those separately, and we will
                # add those paths to the PYTHONPATH for the interpreter. Note that we
                # don't symlink to the parent of the package because that could end up
                # including more stuff we don't want
                self.addl_paths = []
                for p in custom_paths:
                    temp_dir = tempfile.mkdtemp(dir=self.metaflow_dir.name)
                    os.symlink(p, os.path.join(temp_dir, EXT_PKG))
                    self.addl_paths.append(temp_dir)

        # # Also install any environment escape overrides directly here to enable
        # # the escape to work even in non metaflow-created subprocesses
        # from ..env_escape import generate_trampolines
        # generate_trampolines(self.metaflow_dir.name)

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if self.disabled:
            return
        self.interpreter = (
            self.environment.interpreter(self.step)
            if not any(
                decorator.name in ["batch", "kubernetes"]
                for decorator in next(
                    step for step in self.flow if step.name == self.step
                ).decorators
            )
            else None
        )

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        meta,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        if self.disabled:
            return
        # Add Python interpreter's parent to the path to ensure that any non-pythonic
        # dependencies in the virtual environment are visible to the user code.
        # sys.executable points to the Python interpreter in the virtual environment
        # since we are already inside the task context.
        os.environ["PATH"] = os.pathsep.join(
            filter(
                None,
                (
                    os.path.dirname(os.path.realpath(sys.executable)),
                    os.environ.get("PATH"),
                ),
            )
        )

        # Infer environment prefix from Python interpreter
        match = re.search(
            r"(?:.*\/)(metaflow\/[^/]+\/[^/]+)(?=\/bin\/python)", sys.executable
        )
        if match:
            meta.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field="conda_env_prefix",
                        value=match.group(1),
                        type="conda_env_prefix",
                        tags=["attempt_id:{0}".format(retry_count)],
                    )
                ],
            )

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if self.disabled:
            return
        # Ensure local installation of Metaflow is visible to user code
        python_path = self.metaflow_dir.name
        if self.addl_paths is not None:
            addl_paths = os.pathsep.join(self.addl_paths)
            python_path = os.pathsep.join([addl_paths, python_path])
        cli_args.env["PYTHONPATH"] = python_path
        if self.interpreter:
            # https://github.com/conda/conda/issues/7707
            # Also ref - https://github.com/Netflix/metaflow/pull/178
            cli_args.env["PYTHONNOUSERSITE"] = "1"
            # The executable is already in place for the user code to execute against
            cli_args.entrypoint[0] = self.interpreter

    def runtime_finished(self, exception):
        if self.disabled:
            return
        self.metaflow_dir.cleanup()


class CondaFlowDecorator(FlowDecorator):
    """
    Specifies the Conda environment for all steps of the flow.

    Use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    packages : Dict[str, str], default {}
        Packages to use for this flow. The key is the name of the package
        and the value is the version to use.
    libraries : Dict[str, str], default {}
        Supported for backward compatibility. When used with packages, packages will take precedence.
    python : str, optional, default None
        Version of Python to use, e.g. '3.7.4'. A default value of None implies
        that the version used will correspond to the version of the Python interpreter used to start the run.
    disabled : bool, default False
        If set to True, disables Conda.
    """

    # TODO: Migrate conda_base keyword to conda for simplicity.
    name = "conda_base"
    defaults = {
        "packages": {},
        "libraries": {},  # Deprecated! Use packages going forward.
        "python": None,
        "disabled": None,
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(CondaFlowDecorator, self).__init__(attributes, statically_defined)

        # Support legacy 'libraries=' attribute for the decorator.
        self.attributes["packages"] = {
            **self.attributes["libraries"],
            **self.attributes["packages"],
        }
        del self.attributes["libraries"]
        if self.attributes["python"]:
            self.attributes["python"] = str(self.attributes["python"])

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        # @conda uses a conda environment to create a virtual environment.
        # The conda environment can be created through micromamba.
        _supported_virtual_envs = ["conda"]

        # To placate people who don't want to see a shred of conda in UX, we symlink
        # --environment=pypi to --environment=conda
        _supported_virtual_envs.extend(["pypi"])

        # TODO: Hardcoded for now to support Docker environment.
        # We should introduce a more robust mechanism for appending supported environments, for example from within extensions.
        _supported_virtual_envs.extend(["docker"])

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @conda
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    " or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )
