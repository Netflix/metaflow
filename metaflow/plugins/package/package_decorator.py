import os
import sys
import threading
import time

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.package import MetaflowPackage


class PackageDecorator(StepDecorator):
    name = "package"

    def set_remote_step(self, decorators):
        for deco in decorators:
            if deco.name == "titus":
                self.is_remote_step = True
                break

    def __init__(self, attributes=None, statically_defined=False):
        super(PackageDecorator, self).__init__(attributes, statically_defined)
        self.package = None
        self.thread_output = None
        self.is_remote_step = False
        self.lock_acquired = False
        self.package_thread = None

    @classmethod
    def _create_package(
        cls, flow, environment, echo, package_suffixes, flow_datastore, logger
    ):
        try:
            logger(f"Creating package for flow: {flow.name}")
            package = MetaflowPackage(flow, environment, echo, package_suffixes)
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
            logger(
                f"Package created and saved successfully, URL: {cls.package_url}, SHA: {cls.package_sha}"
            )
            cls._package_created = True
            cls.thread_output = "Package created and saved successfully."
        except Exception as e:
            cls.thread_output = f"Package creation failed: {str(e)}"

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        self._environment = environment
        self._flow_datastore = flow_datastore
        self._logger = logger
        self.set_remote_step(decorators)

    def runtime_init(self, flow, graph, package, run_id):
        # Set some more internal state.
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            if self.package.is_package_available and not self.is_remote_step:
                cli_args.env["METAFLOW_CODE_SHA"] = self.package.package_sha
                cli_args.env["METAFLOW_CODE_URL"] = self.package.package_url
                cli_args.env["METAFLOW_CODE_DS"] = self._flow_datastore.TYPE

        self._logger(f"Updated CLI args according to PackageDecorator: {cli_args}")
