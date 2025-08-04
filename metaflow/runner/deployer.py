import os
import json
import time

from typing import ClassVar, Dict, Optional, TYPE_CHECKING

from metaflow.exception import MetaflowNotFound
from metaflow.metaflow_config import DEFAULT_FROM_DEPLOYMENT_IMPL


def generate_fake_flow_file_contents(
    flow_name: str, param_info: dict, project_name: Optional[str] = None
):
    params_code = ""
    for _, param_details in param_info.items():
        param_python_var_name = param_details.get(
            "python_var_name", param_details["name"]
        )
        param_name = param_details["name"]
        param_type = param_details["type"]
        param_help = param_details["description"]
        param_required = param_details["is_required"]

        if param_type == "JSON":
            params_code += (
                f"    {param_python_var_name} = Parameter('{param_name}', "
                f"type=JSONType, help='''{param_help}''', required={param_required})\n"
            )
        elif param_type == "FilePath":
            is_text = param_details.get("is_text", True)
            encoding = param_details.get("encoding", "utf-8")
            params_code += (
                f"    {param_python_var_name} = IncludeFile('{param_name}', "
                f"is_text={is_text}, encoding='{encoding}', help='''{param_help}''', "
                f"required={param_required})\n"
            )
        else:
            params_code += (
                f"    {param_python_var_name} = Parameter('{param_name}', "
                f"type={param_type}, help='''{param_help}''', required={param_required})\n"
            )

    project_decorator = f"@project(name='{project_name}')\n" if project_name else ""

    contents = f"""\
from metaflow import FlowSpec, Parameter, IncludeFile, JSONType, step, project
{project_decorator}class {flow_name}(FlowSpec):
{params_code}
    @step
    def start(self):
        self.next(self.end)
    @step
    def end(self):
        pass
if __name__ == '__main__':
    {flow_name}()
"""
    return contents


if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


class DeployerMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)

        from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

        def _injected_method(method_name, deployer_class):
            def f(self, **deployer_kwargs):
                return deployer_class(
                    deployer_kwargs=deployer_kwargs,
                    flow_file=self.flow_file,
                    show_output=self.show_output,
                    profile=self.profile,
                    env=self.env,
                    cwd=self.cwd,
                    file_read_timeout=self.file_read_timeout,
                    **self.top_level_kwargs,
                )

            f.__doc__ = provider_class.__doc__ or ""
            f.__name__ = method_name
            return f

        for provider_class in DEPLOYER_IMPL_PROVIDERS:
            # TYPE is the name of the CLI groups i.e.
            # `argo-workflows` instead of `argo_workflows`
            # The injected method names replace '-' by '_' though.
            method_name = provider_class.TYPE.replace("-", "_")
            setattr(cls, method_name, _injected_method(method_name, provider_class))

        return cls


class Deployer(metaclass=DeployerMeta):
    """
    Use the `Deployer` class to configure and access one of the production
    orchestrators supported by Metaflow.

    Parameters
    ----------
    flow_file : str
        Path to the flow file to deploy, relative to current directory.
    show_output : bool, default True
        Show the 'stdout' and 'stderr' to the console by default.
    profile : Optional[str], default None
        Metaflow profile to use for the deployment. If not specified, the default
        profile is used.
    env : Optional[Dict[str, str]], default None
        Additional environment variables to set for the deployment.
    cwd : Optional[str], default None
        The directory to run the subprocess in; if not specified, the current
        directory is used.
    file_read_timeout : int, default 3600
        The timeout until which we try to read the deployer attribute file (in seconds).
    **kwargs : Any
        Additional arguments that you would pass to `python myflow.py` before
        the deployment command.
    """

    def __init__(
        self,
        flow_file: str,
        show_output: bool = True,
        profile: Optional[str] = None,
        env: Optional[Dict] = None,
        cwd: Optional[str] = None,
        file_read_timeout: int = 3600,
        **kwargs,
    ):
        # Convert flow_file to absolute path if it's relative
        if not os.path.isabs(flow_file):
            self.flow_file = os.path.abspath(flow_file)
        else:
            self.flow_file = flow_file

        self.show_output = show_output
        self.profile = profile
        self.env = env
        self.cwd = cwd
        self.file_read_timeout = file_read_timeout
        self.top_level_kwargs = kwargs


class TriggeredRun(object):
    """
    TriggeredRun class represents a run that has been triggered on a
    production orchestrator.
    """

    def __init__(
        self,
        deployer: "metaflow.runner.deployer_impl.DeployerImpl",
        content: str,
    ):
        self.deployer = deployer
        content_json = json.loads(content)
        self.metadata_for_flow = content_json.get("metadata")
        self.pathspec = content_json.get("pathspec")
        self.name = content_json.get("name")

    def wait_for_run(self, check_interval: int = 5, timeout: Optional[int] = None):
        """
        Wait for the `run` property to become available.

        The `run` property becomes available only after the `start` task of the triggered
        flow starts running.

        Parameters
        ----------
        check_interval: int, default: 5
            Frequency of checking for the `run` to become available, in seconds.
        timeout : int, optional, default None
            Maximum time to wait for the `run` to become available, in seconds. If
            None, wait indefinitely.

        Raises
        ------
        TimeoutError
            If the `run` is not available within the specified timeout.
        """
        start_time = time.time()
        while True:
            if self.run is not None:
                return self.run

            if timeout is not None and (time.time() - start_time) > timeout:
                raise TimeoutError(
                    "Timed out waiting for the run object to become available."
                )

            time.sleep(check_interval)

    @property
    def run(self) -> Optional["metaflow.Run"]:
        """
        Retrieve the `Run` object for the triggered run.

        Note that Metaflow `Run` becomes available only when the `start` task
        has started executing.

        Returns
        -------
        Run, optional
            Metaflow Run object if the `start` step has started executing, otherwise None.
        """
        from metaflow import Run

        try:
            return Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None


class DeployedFlowMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)
        if not bases:
            # Inject methods only in DeployedFlow and not any of its
            # subclasses
            from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

            allowed_providers = dict(
                {
                    provider.TYPE.replace("-", "_"): provider
                    for provider in DEPLOYER_IMPL_PROVIDERS
                }
            )

            def _get_triggered_run_injected_method():
                def f(
                    cls,
                    identifier: str,
                    run_id: str,
                    metadata: Optional[str] = None,
                    impl: str = DEFAULT_FROM_DEPLOYMENT_IMPL.replace("-", "_"),
                ) -> "TriggeredRun":
                    """
                    Retrieves a `TriggeredRun` object from an identifier, a run id and optional
                    metadata. The `impl` parameter specifies the deployer implementation
                    to use (like `argo-workflows`).

                    Parameters
                    ----------
                    identifier : str
                        Deployer specific identifier for the workflow to retrieve
                    run_id : str
                        Run ID for the which to fetch the triggered run object
                    metadata : str, optional, default None
                        Optional deployer specific metadata.
                    impl : str, optional, default given by METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL
                        The default implementation to use if not specified

                    Returns
                    -------
                    TriggeredRun
                        A `TriggeredRun` object representing the triggered run corresponding
                        to the identifier and the run id.
                    """
                    if impl in allowed_providers:
                        return (
                            allowed_providers[impl]
                            .deployed_flow_type()
                            .get_triggered_run(identifier, run_id, metadata)
                        )
                    else:
                        raise ValueError(
                            f"No deployer '{impl}' exists; valid deployers are: "
                            f"{list(allowed_providers.keys())}"
                        )

                f.__name__ = "get_triggered_run"
                return f

            def _per_type_get_triggered_run_injected_method(method_name, impl):
                def f(
                    cls,
                    identifier: str,
                    run_id: str,
                    metadata: Optional[str] = None,
                ):
                    return (
                        allowed_providers[impl]
                        .deployed_flow_type()
                        .get_triggered_run(identifier, run_id, metadata)
                    )

                f.__name__ = method_name
                return f

            def _from_deployment_injected_method():
                def f(
                    cls,
                    identifier: str,
                    metadata: Optional[str] = None,
                    impl: str = DEFAULT_FROM_DEPLOYMENT_IMPL.replace("-", "_"),
                ) -> "DeployedFlow":
                    """
                    Retrieves a `DeployedFlow` object from an identifier and optional
                    metadata. The `impl` parameter specifies the deployer implementation
                    to use (like `argo-workflows`).

                    Parameters
                    ----------
                    identifier : str
                        Deployer specific identifier for the workflow to retrieve
                    metadata : str, optional, default None
                        Optional deployer specific metadata.
                    impl : str, optional, default given by METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL
                        The default implementation to use if not specified

                    Returns
                    -------
                    DeployedFlow
                        A `DeployedFlow` object representing the deployed flow corresponding
                        to the identifier
                    """
                    if impl in allowed_providers:
                        return (
                            allowed_providers[impl]
                            .deployed_flow_type()
                            .from_deployment(identifier, metadata)
                        )
                    else:
                        raise ValueError(
                            f"No deployer '{impl}' exists; valid deployers are: "
                            f"{list(allowed_providers.keys())}"
                        )

                f.__name__ = "from_deployment"
                return f

            def _per_type_from_deployment_injected_method(method_name, impl):
                def f(
                    cls,
                    identifier: str,
                    metadata: Optional[str] = None,
                ):
                    return (
                        allowed_providers[impl]
                        .deployed_flow_type()
                        .from_deployment(identifier, metadata)
                    )

                f.__name__ = method_name
                return f

            def _list_deployed_flows_injected_method():
                def f(
                    cls,
                    flow_name: Optional[str] = None,
                    impl: str = DEFAULT_FROM_DEPLOYMENT_IMPL.replace("-", "_"),
                ):
                    """
                    List all deployed flows for the specified implementation.

                    Parameters
                    ----------
                    flow_name : str, optional, default None
                        If specified, only list deployed flows for this specific flow name.
                        If None, list all deployed flows.
                    impl : str, optional, default given by METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL
                        The default implementation to use if not specified

                    Yields
                    ------
                    DeployedFlow
                        `DeployedFlow` objects representing deployed flows.
                    """
                    if impl in allowed_providers:
                        return (
                            allowed_providers[impl]
                            .deployed_flow_type()
                            .list_deployed_flows(flow_name)
                        )
                    else:
                        raise ValueError(
                            f"No deployer '{impl}' exists; valid deployers are: "
                            f"{list(allowed_providers.keys())}"
                        )

                f.__name__ = "list_deployed_flows"
                return f

            def _per_type_list_deployed_flows_injected_method(method_name, impl):
                def f(
                    cls,
                    flow_name: Optional[str] = None,
                ):
                    return (
                        allowed_providers[impl]
                        .deployed_flow_type()
                        .list_deployed_flows(flow_name)
                    )

                f.__name__ = method_name
                return f

            setattr(
                cls, "from_deployment", classmethod(_from_deployment_injected_method())
            )
            setattr(
                cls,
                "list_deployed_flows",
                classmethod(_list_deployed_flows_injected_method()),
            )
            setattr(
                cls,
                "get_triggered_run",
                classmethod(_get_triggered_run_injected_method()),
            )

            for impl in allowed_providers:
                from_deployment_method_name = f"from_{impl}"
                list_deployed_flows_method_name = f"list_{impl}"
                get_triggered_run_method_name = f"get_triggered_{impl}_run"

                setattr(
                    cls,
                    from_deployment_method_name,
                    classmethod(
                        _per_type_from_deployment_injected_method(
                            from_deployment_method_name, impl
                        )
                    ),
                )

                setattr(
                    cls,
                    list_deployed_flows_method_name,
                    classmethod(
                        _per_type_list_deployed_flows_injected_method(
                            list_deployed_flows_method_name, impl
                        )
                    ),
                )

                setattr(
                    cls,
                    get_triggered_run_method_name,
                    classmethod(
                        _per_type_get_triggered_run_injected_method(
                            get_triggered_run_method_name, impl
                        )
                    ),
                )

        return cls


class DeployedFlow(metaclass=DeployedFlowMeta):
    """
    DeployedFlow class represents a flow that has been deployed.

    This class is not meant to be instantiated directly. Instead, it is returned from
    methods of `Deployer`.
    """

    # This should match the TYPE value in DeployerImpl for proper stub generation
    TYPE: ClassVar[Optional[str]] = None

    def __init__(self, deployer: "metaflow.runner.deployer_impl.DeployerImpl"):
        self.deployer = deployer
        self.name = self.deployer.name
        self.flow_name = self.deployer.flow_name
        self.metadata = self.deployer.metadata
