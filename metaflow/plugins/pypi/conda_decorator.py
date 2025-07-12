import os
import platform
import re
import sys
import tempfile

from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.metadata_provider import MetaDatum
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.packaging_sys import ContentType


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

    _metaflow_home = None
    _addl_env_vars = None

    # To define conda channels for the whole solve, users can specify
    # CONDA_CHANNELS in their environment. For pinning specific packages to specific
    # conda channels, users can specify channel::package as the package name.

    def __init__(self, attributes=None, statically_defined=False, inserted_by=None):
        self._attributes_with_user_values = (
            set(attributes.keys()) if attributes is not None else set()
        )

        super(CondaStepDecorator, self).__init__(
            attributes, statically_defined, inserted_by
        )

    def init(self):
        # Support legacy 'libraries=' attribute for the decorator.
        self.attributes["packages"] = {
            **self.attributes["libraries"],
            **self.attributes["packages"],
        }
        # Keep because otherwise make_decorator_spec will fail
        self.attributes["libraries"] = {}
        if self.attributes["packages"]:
            self._attributes_with_user_values.add("packages")

    def is_attribute_user_defined(self, name):
        return name in self._attributes_with_user_values

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
            conda_base = self.flow._flow_decorators["conda_base"][0]
            super_attributes = conda_base.attributes
            self.attributes["packages"] = {
                **super_attributes["packages"],
                **self.attributes["packages"],
            }
            self._attributes_with_user_values.update(
                conda_base._attributes_with_user_values
            )

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

        # TODO: Hardcoded for now to support the fast bakery environment.
        # We should introduce a more robust mechanism for appending supported environments, for example from within extensions.
        _supported_virtual_envs.extend(["fast-bakery"])

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
        # We need to make all the code package available to the user code in
        # a temporary directory which will be added to the PYTHONPATH.
        if self.__class__._metaflow_home is None:
            # Do this ONCE per flow
            self.__class__._metaflow_home = tempfile.TemporaryDirectory(dir="/tmp")
            package.extract_into(
                self.__class__._metaflow_home.name, ContentType.ALL_CONTENT
            )
            self.__class__._addl_env_vars = package.get_post_extract_env_vars(
                package.package_metadata, self.__class__._metaflow_home.name
            )

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
                decorator.name
                in ["batch", "kubernetes", "nvidia", "snowpark", "slurm", "nvct"]
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
        python_path = self.__class__._metaflow_home.name
        addl_env_vars = {}
        if self.__class__._addl_env_vars is not None:
            for key, value in self.__class__._addl_env_vars.items():
                if key == "PYTHONPATH":
                    addl_env_vars[key] = os.pathsep.join([value, python_path])
                else:
                    addl_env_vars[key] = value
        cli_args.env.update(addl_env_vars)
        if self.interpreter:
            # https://github.com/conda/conda/issues/7707
            # Also ref - https://github.com/Netflix/metaflow/pull/178
            cli_args.env["PYTHONNOUSERSITE"] = "1"
            # The executable is already in place for the user code to execute against
            cli_args.entrypoint[0] = self.interpreter

    def runtime_finished(self, exception):
        if self.disabled:
            return
        if self.__class__._metaflow_home is not None:
            self.__class__._metaflow_home.cleanup()
            self.__class__._metaflow_home = None


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

    def __init__(self, attributes=None, statically_defined=False, inserted_by=None):
        self._attributes_with_user_values = (
            set(attributes.keys()) if attributes is not None else set()
        )

        super(CondaFlowDecorator, self).__init__(
            attributes, statically_defined, inserted_by
        )

    def init(self):
        # Support legacy 'libraries=' attribute for the decorator.
        self.attributes["packages"] = {
            **self.attributes["libraries"],
            **self.attributes["packages"],
        }
        # Keep because otherwise make_decorator_spec will fail
        self.attributes["libraries"] = {}
        if self.attributes["python"]:
            self.attributes["python"] = str(self.attributes["python"])

    def is_attribute_user_defined(self, name):
        return name in self._attributes_with_user_values

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        # NOTE: Important for extensions implementing custom virtual environments.
        # Without this steps will not have an implicit conda step decorator on them unless the environment adds one in its decospecs.
        from metaflow import decorators

        decorators._attach_decorators(flow, ["conda"])

        # @conda uses a conda environment to create a virtual environment.
        # The conda environment can be created through micromamba.
        _supported_virtual_envs = ["conda"]

        # To placate people who don't want to see a shred of conda in UX, we symlink
        # --environment=pypi to --environment=conda
        _supported_virtual_envs.extend(["pypi"])

        # TODO: Hardcoded for now to support the fast bakery environment.
        # We should introduce a more robust mechanism for appending supported environments, for example from within extensions.
        _supported_virtual_envs.extend(["fast-bakery"])

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
