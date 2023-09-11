import os
import platform
import sys
import tempfile

from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.util import get_metaflow_root


class CondaStepDecorator(StepDecorator):
    name = "conda"
    defaults = {
        "packages": {},
        "libraries": {},  # Deprecated! Use packages going forward
        "python": None,
        # TODO: Add support for disabled
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
        # @conda uses a conda environment to create a virtual environment.
        # The conda environment can be created through micromamba.
        _supported_virtual_envs = ["conda"]

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @conda
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    "or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )

        # At this point, the list of 32 bit instance types is shrinking quite rapidly.
        # We can worry about supporting them when there is a need.

        # The init_environment hook for Environment creates the relevant virtual
        # environments. The step_init hook sets up the relevant state for that hook to
        # do it's magic.

        self.flow = flow
        self.step = step
        self.environment = environment
        self.datastore = flow_datastore

        # TODO: This code snippet can be done away with by altering the constructor of
        #       MetaflowEnvironment. A good first-task exercise.
        # Avoid circular import
        from metaflow.plugins.datastores.local_storage import LocalStorage

        environment.set_local_root(LocalStorage.get_datastore_root_from_config(logger))

        # Support flow-level decorator
        if "conda_base" in self.flow._flow_decorators:
            super_attributes = self.flow._flow_decorators["conda_base"][0].attributes
            self.attributes["packages"] = {
                **super_attributes["packages"],
                **self.attributes["packages"],
            }
            self.attributes["python"] = (
                self.attributes["python"] or super_attributes["python"]
            )

        # Set Python interpreter to user's Python if necessary.
        if not self.attributes["python"]:
            self.attributes["python"] = platform.python_version()  # CPython!

    def runtime_init(self, flow, graph, package, run_id):
        # Create a symlink to metaflow installed outside the virtual environment
        self.metaflow_dir = tempfile.TemporaryDirectory(dir="/tmp")
        os.symlink(
            os.path.join(get_metaflow_root(), "metaflow"),
            os.path.join(self.metaflow_dir.name, "metaflow"),
        )

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        # TODO: Consider recreating the environment if the environment is missing
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
        # TODO: Register metadata

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        # TODO: Support unbounded for-each
        # TODO: Check what happens when PYTHONPATH is defined via @environment
        # Ensure local installation of Metaflow is visible to user code
        cli_args.env["PYTHONPATH"] = self.metaflow_dir.name
        # TODO: Verify user site-package isolation behavior
        #       https://github.com/conda/conda/issues/7707
        #       Also ref - https://github.com/Netflix/metaflow/pull/178
        # cli_args.env["PYTHONNOUSERSITE"] = "1"
        # The executable is already in place for the user code to execute against
        if self.interpreter:
            cli_args.entrypoint[0] = self.interpreter

    def runtime_finished(self, exception):
        self.metaflow_dir.cleanup()


class CondaFlowDecorator(FlowDecorator):
    # TODO: Migrate conda_base keyword to conda for simplicity.
    name = "conda_base"
    defaults = {
        "packages": {},
        "libraries": {},  # Deprecated! Use packages going forward.
        "python": None,
        # TODO: Add support for disabled
        # TODO: Support `@conda(python='3.10')` before shipping!!
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

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @conda
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    "or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )
