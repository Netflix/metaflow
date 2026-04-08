from enum import Enum
from typing import Optional, Dict, List, Any, Callable, TYPE_CHECKING


class ExecutionPhase(Enum):
    """
    Represents which process phase Metaflow is currently executing in.

    Metaflow can execute across up to three separate processes:

    LAUNCH: The initial `myflow.py` process. This is where the flow is parsed,
        decorators are initialized, code is packaged, and (for local runs) the
        runtime scheduler operates. Hooks called here are:
          - For FlowDecorators: `flow_init`
          - For StepDecorators: `step_init`, `package_init`, `runtime_*`
        For deployment commands (e.g. `argo-workflows create`), only `flow_init`,
        `step_init` and `package_init` are called.

    TRAMPOLINE: A per-task subprocess that delegates execution to a remote system
        (e.g. AWS Batch, Kubernetes). Hooks called here are:
          - For FlowDecorators: `flow_init`
          - For StepDecorators: `step_init`
        This phase does NOT exist for local
        runs (where the subprocess directly executes user code) or for Argo/Step
        Functions deployments (where the orchestrator launches tasks directly).

    TASK: The process where user code actually runs. Hooks called here are:
           - For FlowDecorators: `flow_init`
           - For StepDecorators: `step_init`, `task_*`
        For local runs, this is the per-task subprocess. For remote runs, this is
        the process running on Batch/Kubernetes/etc.
    """

    LAUNCH = "launch"
    TRAMPOLINE = "trampoline"
    TASK = "task"


# Commands that indicate the process is in the TASK phase.
_TASK_COMMANDS = frozenset({"step", "init", "spin-step"})


if TYPE_CHECKING:
    from metaflow.datastore.flow_datastore import FlowDataStore
    from metaflow.datastore.task_datastore import TaskDataStore
    from metaflow.flowspec import FlowSpec
    from metaflow.graph import FlowGraph
    from metaflow.metadata_provider.metadata import MetadataProvider
    from metaflow.metaflow_environment import MetaflowEnvironment
    from metaflow.package.metaflow_package import MetaflowPackage


def _phase_from_cli_args(saved_args: Optional[List[str]]):
    """
    Determine the execution phase from the CLI's saved_args.

    Parameters
    ----------
    saved_args : List[str], optional
        The remaining CLI arguments after the top-level group has been parsed.
        The first element identifies the subcommand (e.g. "run", "step",
        "batch", "kubernetes").

    Returns
    -------
    ExecutionPhase
    """
    if not saved_args:
        return ExecutionPhase.LAUNCH

    from metaflow.plugins import get_trampoline_cli_names

    first_arg = saved_args[0]
    if first_arg in _TASK_COMMANDS:
        return ExecutionPhase.TASK
    elif first_arg in get_trampoline_cli_names():
        return ExecutionPhase.TRAMPOLINE
    else:
        return ExecutionPhase.LAUNCH


class SystemContext:
    """
    Singleton holding infrastructure/system data and the current execution phase.

    This object is created once per process and progressively populated by the
    runtime as information becomes available. Decorators access it via the
    ``system_ctx`` property on the decorator base class.

    1. **Phase awareness**: decorators can check which execution phase they are
       in, allowing the same hook implementation (e.g. ``step_init``) to behave
       differently in the launch process vs. a task subprocess.

    2. **Progressive updates**: the runtime system calls ``_update()`` to add
       information as it becomes available (e.g. ``run_id`` is set at
       ``runtime_init`` time, ``task_id`` at ``task_pre_step`` time).
    """

    def __init__(self):
        # Flow-level — available in all hooks in all phases
        self._phase = None
        self._flow = None
        self._graph = None
        self._environment = None
        self._flow_datastore = None
        self._metadata = None
        self._logger = None

        # Step-level information -- available for all step-decorators in all phases
        self._step_name = None
        # Inter-decorator shared state (keyed by step_name)
        self._shared = {}  # { step_name: { namespace: { key: value } } }
        self._step_decorators = {}  # { step_name: [StepDecorator, ...] }

        # Package-level - available in package_init (LAUNCH phase *only*)
        self._package = None

        # Runtime-level — available in runtime_init (LAUNCH phase) and task_* (TASK phase)
        self._run_id = None

        # Task-level - available in runtime_task_created, runtime_task_cli (LAUNCH phase) and task_* (TASK phase)
        self._input_paths = None
        self._task_id = None
        self._task_datastore = None
        self._ubf_context = None
        self._is_cloned = None  # NOTE: For now, not available in task_* hooks
        self._split_index = None

        # Retry information - available in runtime_step_cli (LAUNCH phase) and task_* (TASK phase)
        self._max_user_code_retries = None
        self._retry_count = None

        # Task only information - available in task_* hooks (TASK phase)
        self._inputs = None

    # ------------------------------------------------------------------
    # Phase queries
    # ------------------------------------------------------------------

    @property
    def phase(self) -> "ExecutionPhase":
        """The current :class:`ExecutionPhase`."""
        return self._phase

    @property
    def is_launch(self) -> bool:
        """True if executing in the initial CLI / orchestrator process."""
        return self._phase == ExecutionPhase.LAUNCH

    @property
    def is_trampoline(self) -> bool:
        """True if executing in a per-task subprocess that delegates remotely."""
        return self._phase == ExecutionPhase.TRAMPOLINE

    @property
    def is_task(self) -> bool:
        """True if executing in the process where user code runs."""
        return self._phase == ExecutionPhase.TASK

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def flow(self) -> Optional[Any]:
        """The FlowSpec instance (available after ``flow_init``)."""
        return self._flow

    @property
    def graph(self) -> Optional[Any]:
        """The FlowGraph (available after ``flow_init``)."""
        return self._graph

    @property
    def environment(self) -> Optional[Any]:
        """The MetaflowEnvironment instance."""
        return self._environment

    @property
    def flow_datastore(self) -> Optional[Any]:
        """The FlowDataStore instance."""
        return self._flow_datastore

    @property
    def logger(self) -> Optional[Callable[..., Any]]:
        """The logger callable."""
        return self._logger

    @property
    def metadata(self) -> Optional[Any]:
        """The metadata provider."""
        return self._metadata

    @property
    def step_name(self) -> Optional[str]:
        """Current step name (available after ``step_init``)."""
        return self._step_name

    @property
    def input_paths(self) -> Optional[List[str]]:
        """List of input pathspec strings (available after ``runtime_task_created``)."""
        return self._input_paths

    @property
    def package(self) -> Optional[Any]:
        """The code package (available after ``runtime_init``, LAUNCH only)."""
        return self._package

    @property
    def run_id(self) -> Optional[str]:
        """The run ID (available after ``runtime_init`` or ``task_pre_step``)."""
        return self._run_id

    @property
    def task_id(self) -> Optional[str]:
        """The task ID (available during task hooks)."""
        return self._task_id

    @property
    def task_datastore(self) -> Optional[Any]:
        """The task's output datastore (available during task hooks)."""
        return self._task_datastore

    @property
    def retry_count(self) -> Optional[int]:
        """Current retry attempt number (available during task hooks)."""
        return self._retry_count

    @property
    def max_user_code_retries(self) -> Optional[int]:
        """Maximum user code retries configured (available during task hooks)."""
        return self._max_user_code_retries

    @property
    def ubf_context(self) -> Optional[Any]:
        """Unbounded foreach context (available during task hooks)."""
        return self._ubf_context

    @property
    def is_cloned(self) -> Optional[bool]:
        """Whether the task is resumed from a prior run (available after ``runtime_task_created``)."""
        return self._is_cloned

    @property
    def inputs(self) -> Optional[List[Any]]:
        """List of input datastores (available during task hooks)."""
        return self._inputs

    @property
    def split_index(self) -> Optional[int]:
        """Foreach split index (available during task hooks)."""
        return self._split_index

    # ------------------------------------------------------------------
    # Step decorator registration
    # ------------------------------------------------------------------

    def register_step_decorators(self, step_name: str, decorators: List[Any]) -> None:
        """Register the list of decorator instances for a step."""
        self._step_decorators[step_name] = list(decorators)

    def get_step_decorators(self, step_name: Optional[str] = None) -> List[Any]:
        """
        Return the decorator instances for a step.

        If *step_name* is None, uses the current step.
        """
        if step_name is None:
            step_name = self._step_name
        return list(self._step_decorators.get(step_name, []))

    # ------------------------------------------------------------------
    # Inter-decorator shared state
    # ------------------------------------------------------------------

    def publish(self, namespace: str, key: str, value: Any) -> None:
        """
        Publish a value for other decorators to read.

        By convention, use the decorator's ``name`` as the namespace to avoid
        collisions. The value is scoped to the current step.

        Parameters
        ----------
        namespace : str
            Typically the publishing decorator's name.
        key : str
            Identifier for the value within the namespace.
        value : any
            The value to publish.
        """
        step = self._step_name
        if step not in self._shared:
            self._shared[step] = {}
        if namespace not in self._shared[step]:
            self._shared[step][namespace] = {}
        self._shared[step][namespace][key] = value

    def get_published(self, namespace: str, key: str, default: Any = None) -> Any:
        """
        Read a value published by another decorator.

        Parameters
        ----------
        namespace : str
            The publishing decorator's namespace.
        key : str
            The key to look up.
        default : any
            Returned if the namespace or key is not found.

        Returns
        -------
        any
        """
        step = self._step_name
        return self._shared.get(step, {}).get(namespace, {}).get(key, default)

    def has_published(self, namespace: str, key: Optional[str] = None) -> bool:
        """
        Check whether a decorator has published data.

        Parameters
        ----------
        namespace : str
        key : str or None
            If None, checks whether the namespace exists at all.

        Returns
        -------
        bool
        """
        step = self._step_name
        step_shared = self._shared.get(step, {})
        if key is None:
            return namespace in step_shared
        return key in step_shared.get(namespace, {})

    def get_all_published(self, namespace: str) -> Dict[str, Any]:
        """
        Return all key-value pairs published under a namespace.

        Parameters
        ----------
        namespace : str

        Returns
        -------
        dict
        """
        step = self._step_name
        return dict(self._shared.get(step, {}).get(namespace, {}))

    # ------------------------------------------------------------------
    # Progressive update (called by the runtime, not by decorators)
    # ------------------------------------------------------------------

    def _update(self, **kwargs: Any) -> None:
        """
        Set or update context values.

        The runtime calls this at various lifecycle stages to add information
        as it becomes available. For example, ``run_id`` is set at
        ``runtime_init`` time, and ``task_id`` is set at ``task_pre_step`` time.
        """
        for k, v in kwargs.items():
            attr = f"_{k}"
            if not hasattr(self, attr):
                raise AttributeError(f"SystemContext has no attribute {k!r}")
            setattr(self, attr, v)

    def _reset(self) -> None:
        """Reset all state. Intended for test cleanup only."""
        self.__init__()


system_context = SystemContext()
