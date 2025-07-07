import json
import os
import platform
import sys

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    NOMAD_DEFAULT_REGION,
    NOMAD_DEFAULT_DATACENTERS,
    NOMAD_DEFAULT_IMAGE,
    METAFLOW_NOMAD_CPU,
    METAFLOW_NOMAD_MEMORY,
    METAFLOW_NOMAD_DISK,
    # Add other Nomad-specific config variables here
)
from metaflow.plugins.resources_decorator import ResourcesDecorator
# from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task # If we add timeout support

# Define a new exception type for Nomad specific errors
class NomadException(MetaflowException):
    headline = "Nomad error"

class NomadDecorator(StepDecorator):
    """
    Specifies that this step should execute on HashiCorp Nomad.

    Parameters
    ----------
    region : str, optional
        Nomad region to run the job in. Defaults to `NOMAD_DEFAULT_REGION` from Metaflow config.
    datacenters : list[str], optional
        List of Nomad datacenters to run the job in. Defaults to `NOMAD_DEFAULT_DATACENTERS` from Metaflow config.
    constraints : list[dict], optional
        Nomad constraints to apply to the job.
        Example: `[{"attribute": "${attr.kernel.name}", "operator": "=", "value": "linux"}]`
        See https://developer.hashicorp.com/nomad/docs/job-specification/constraint.
    image : str, optional
        Docker image to use for the job. Defaults to `NOMAD_DEFAULT_IMAGE` from Metaflow config or a Python version based image.
    cpu : int, optional
        CPU resources to request for the job (in MHz). This will override `@resources` if specified.
    memory : int, optional
        Memory resources to request for the job (in MB). This will override `@resources` if specified.
    disk : int, optional
        Disk resources to request for the job (in MB). This will override `@resources` if specified.
    # Add other Nomad-specific parameters here, e.g., network, port_map
    """
    name = "nomad"
    defaults = {
        "region": None,
        "datacenters": None,
        "constraints": None,
        "image": None,
        "cpu": None,
        "memory": None,
        "disk": None,
        # "run_time_limit": 5 * 24 * 60 * 60,  # Default 5 days, if timeout decorator is used
    }

    package_url = None
    package_sha = None
    run_time_limit = None # Will be set by timeout decorator logic if implemented

    def init(self):
        super(NomadDecorator, self).init()
        # Initialize attributes from defaults or Metaflow config
        if self.attributes["region"] is None:
            self.attributes["region"] = NOMAD_DEFAULT_REGION
        if self.attributes["datacenters"] is None:
            # Ensure datacenters is a list
            datacenters_cfg = NOMAD_DEFAULT_DATACENTERS
            if isinstance(datacenters_cfg, str):
                datacenters_cfg = [dc.strip() for dc in datacenters_cfg.split(',') if dc.strip()]
            self.attributes["datacenters"] = datacenters_cfg

        if self.attributes["image"] is None:
            if NOMAD_DEFAULT_IMAGE:
                self.attributes["image"] = NOMAD_DEFAULT_IMAGE
            else:
                self.attributes["image"] = "python:%s.%s-slim" % (
                    platform.python_version_tuple()[0],
                    platform.python_version_tuple()[1],
                )

        # Set default resource values from metaflow_config if not set by decorator
        # These might be overridden by @resources decorator later in step_init
        if self.attributes["cpu"] is None and METAFLOW_NOMAD_CPU:
            self.attributes["cpu"] = METAFLOW_NOMAD_CPU
        if self.attributes["memory"] is None and METAFLOW_NOMAD_MEMORY:
            self.attributes["memory"] = METAFLOW_NOMAD_MEMORY
        if self.attributes["disk"] is None and METAFLOW_NOMAD_DISK:
            self.attributes["disk"] = METAFLOW_NOMAD_DISK


    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

        if flow_datastore.TYPE not in ("s3", "azure", "gs"):
            raise NomadException(
                "The *@nomad* decorator requires --datastore=s3, --datastore=azure, or --datastore=gs."
            )

        # self.run_time_limit = get_run_time_limit_for_task(decos) # If timeout decorator is used
        # if self.run_time_limit < 60: # Or some other minimum
        #     raise NomadException("The timeout for step *%s* should be at least 60 seconds for execution on Nomad." % step)

        # Override with @resources decorator if present
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    if k in self.attributes and v is not None:
                        # If @nomad also specifies cpu/memory/disk, @resources takes precedence
                        # or we can choose to let @nomad override @resources by checking self.attributes[k] is None
                        if self.attributes[k] is None or k not in self.defaults or self.attributes[k] == self.defaults[k]:
                             self.attributes[k] = v
                        else:
                            # If both @nomad and @resources specify a resource, take the max
                            # This behavior matches KubernetesDecorator
                            try:
                                self.attributes[k] = str(max(float(self.attributes[k] or 0), float(v or 0)))
                            except ValueError:
                                # Handle cases where conversion to float might fail (e.g. for non-numeric attributes)
                                self.attributes[k] = v


        # Validate resource attributes (ensure they are positive numbers if set)
        for attr in ["cpu", "memory", "disk"]:
            if self.attributes[attr] is not None:
                try:
                    val = float(self.attributes[attr])
                    if val <= 0:
                        raise ValueError
                except (TypeError, ValueError):
                    raise NomadException(
                        "Invalid {} value *{}* for step *{}*; it should be a positive number.".format(
                            attr, self.attributes[attr], step
                        )
                    )

        # Validate constraints format if provided
        if self.attributes["constraints"] is not None:
            if not isinstance(self.attributes["constraints"], list):
                raise NomadException("Nomad 'constraints' attribute must be a list of dicts.")
            for constraint in self.attributes["constraints"]:
                if not isinstance(constraint, dict):
                    raise NomadException("Each item in Nomad 'constraints' must be a dict.")
                # Further validation of constraint keys can be added here


    def runtime_init(self, flow, graph, package, run_id):
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        if retry_count <= max_user_code_retries:
            cli_args.commands = ["nomad", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)

            # Pass all attributes of the decorator as command options
            for k, v in self.attributes.items():
                if v is not None:
                    if isinstance(v, (list, dict)):
                         cli_args.command_options[k.replace("_", "-")] = json.dumps(v)
                    else:
                        cli_args.command_options[k.replace("_", "-")] = v

            # if self.run_time_limit: # If timeout decorator is used
            #    cli_args.command_options["run-time-limit"] = self.run_time_limit

            # Ensure the entrypoint uses the python executable from the environment
            # This might need adjustment based on how the Nomad task driver executes the command
            cli_args.entrypoint = [self.environment.executable(self.step)]


    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            # this ensures that we upload the code package only once
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]

# Placeholder for functions to be added to metaflow_config.py
# Ensure these are actually added to the real metaflow_config.py file

# NOMAD_DEFAULT_REGION = os.environ.get("METAFLOW_NOMAD_DEFAULT_REGION", "global")
# NOMAD_DEFAULT_DATACENTERS = os.environ.get("METAFLOW_NOMAD_DEFAULT_DATACENTERS", "dc1") # Comma-separated string e.g. "dc1,dc2"
# NOMAD_DEFAULT_IMAGE = os.environ.get("METAFLOW_NOMAD_DEFAULT_IMAGE", None)
# METAFLOW_NOMAD_CPU = os.environ.get("METAFLOW_NOMAD_CPU", "1000") # Default CPU in MHz for Nomad tasks
# METAFLOW_NOMAD_MEMORY = os.environ.get("METAFLOW_NOMAD_MEMORY", "4096") # Default Memory in MB for Nomad tasks
# METAFLOW_NOMAD_DISK = os.environ.get("METAFLOW_NOMAD_DISK", "10240") # Default Disk in MB for Nomad tasks
# To be added to metaflow_config_funcs.py in ALL_VALUE_OVERRIDES
# ("nomad_default_region", "NOMAD_DEFAULT_REGION"),
# ("nomad_default_datacenters", "NOMAD_DEFAULT_DATACENTERS"),
# ("nomad_default_image", "NOMAD_DEFAULT_IMAGE"),
# ("nomad_cpu", "METAFLOW_NOMAD_CPU"),
# ("nomad_memory", "METAFLOW_NOMAD_MEMORY"),
# ("nomad_disk", "METAFLOW_NOMAD_DISK"),
