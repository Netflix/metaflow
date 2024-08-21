import os
import sys
import threading
import time

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.package import MetaflowPackage


class PackageDecorator(StepDecorator):
    name = "package"
    # Instance variables as they need to be shared across all instances of the decorator
    # in the main/initial process.
    package_url = None
    package_sha = None
    _lock = threading.Lock()
    _package_created = False

    @staticmethod
    def _is_executing_remotely():
        return os.environ.get("TITUS_TASK_ID", None) is not None

    @staticmethod
    def _is_subprocess():
        return os.environ.get("METAFLOW_SUBPROCESS", "0") == "1"

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
        self._flow_datastore = flow_datastore
        self._environment = environment
        self._logger = logger
        self.package_suffixes = self.attributes.get(
            "package_suffixes", flow.package_suffixes
        )
        self.set_remote_step(decorators)

        self._logger("-" * 100)
        self._logger(
            f"Flow: {flow.name}, Step: {step_name}, Package suffixes: {self.package_suffixes}"
        )
        self._logger(
            f"Is subprocess: {self._is_subprocess()}, Is remote step: {self.is_remote_step}, Is executing remotely: {self._is_executing_remotely()}"
        )
        if not self._is_subprocess() and not self._is_executing_remotely():
            if self._lock.acquire(blocking=False):
                self.lock_acquired = True
                self._logger(f"{step_name} acquired lock.")
                if not self._package_created:
                    self._logger(f"{step_name} is creating package.")
                    self.package_thread = threading.Thread(
                        target=self._create_package,
                        args=(
                            flow,
                            environment,
                            flow.echo,
                            self.package_suffixes,
                            flow_datastore,
                            self._logger,
                        ),
                    )
                    self.package_thread.start()
                else:
                    self._logger(f"{step_name} found package already created.")
            else:
                self._logger(
                    f"{step_name} couldn't acquire lock. Another thread is already packaging the flow."
                )

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        self._logger("-" * 100)
        self._logger(f"PackageDecorator, Runtime step CLI: {cli_args}")
        step_name = cli_args.command_args[-1]
        self._logger(
            f"Is subprocess: {self._is_subprocess()}, Step Name: {step_name}, requires_package: {self.is_remote_step}"
        )
        self._logger(
            f"Package URL: {self.package_url}, Package SHA: {self.package_sha}"
        )

        if retry_count <= max_user_code_retries:
            max_wait_time = 300
            wait_interval = 5
            total_wait_time = 0

            if self.is_remote_step:
                while (
                    not self.package_url or not self.package_sha
                ) and total_wait_time < max_wait_time:
                    time.sleep(wait_interval)
                    total_wait_time += wait_interval
                    self._logger(
                        f"Waiting for package to be created... ({total_wait_time}s)"
                    )

                if not self.package_url or not self.package_sha:
                    raise MetaflowException(
                        "Package creation failed. Please check the logs for more information."
                    )

            self._logger(
                f"After Waiting Package URL: {self.package_url}, Package SHA: {self.package_sha}"
            )
            if self.package_url and self.package_sha:
                cli_args.env["METAFLOW_CODE_SHA"] = self.package_sha
                cli_args.env["METAFLOW_CODE_URL"] = self.package_url
                cli_args.env["METAFLOW_CODE_DS"] = self._flow_datastore.TYPE
                if self.is_remote_step:
                    cli_args.command_args.append(self.package_sha)
                    cli_args.command_args.append(self.package_url)

        self._logger(f"Updated CLI args according to PackageDecorator: {cli_args}")
        self._logger("-" * 100)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        # Join only after the task is finished, so that other decorators do not
        # acquire the lock and start creating the package again.
        if self.package_thread:
            self.package_thread.join()
            print(f"Package thread joined. Output: {self.thread_output}")

        if self.lock_acquired:
            self._lock.release()
            print(f"{step_name} released lock in task_finished.")
