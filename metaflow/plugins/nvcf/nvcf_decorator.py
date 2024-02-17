import os
import sys

from metaflow.decorators import StepDecorator
from metaflow.metadata.util import sync_local_metadata_to_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.sidecar import Sidecar

from .nvcf import NvcfException


class NvcfDecorator(StepDecorator):
    name = "nvcf"
    defaults = {"function_id": "9e5647f2-740f-4101-a129-1c961a075575"}

    package_url = None
    package_sha = None

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    # to understand where these functions are invoked in the lifecycle of a
    # Metaflow flow.
    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        if flow_datastore.TYPE != "s3":
            raise NvcfException("The *@nvcf* decorator requires --datastore=s3.")
        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore

    def runtime_init(self, flow, graph, package, run_id):
        # Set some more internal state.
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, task_datastore, task_id, split_index, input_paths, is_cloned, ubf_context
    ):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if retry_count <= max_user_code_retries:
            # after all attempts to run the user code have failed, we don't need
            # to execute on NVCF anymore. We can execute possible fallback
            # code locally.
            cli_args.commands = ["nvcf", "step"]
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        self.metadata = metadata
        self.task_datastore = task_datastore

        # task_pre_step may run locally if fallback is activated for @catch
        # decorator.

        if "NVCF_REQUEST_ID" in os.environ:
            self._save_logs_sidecar = Sidecar("save_logs_periodically")
            self._save_logs_sidecar.start()

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_retries
    ):
        # task_finished may run locally if fallback is activated for @catch
        # decorator.
        if "NVCF_REQUEST_ID" in os.environ:
            # If `local` metadata is configured, we would need to copy task
            # execution metadata from the NVCF container to user's
            # local file system after the user code has finished execution.
            # This happens via datastore as a communication bridge.
            if hasattr(self, "metadata") and self.metadata.TYPE == "local":
                # Note that the datastore is *always* Amazon S3 (see
                # runtime_task_created function).
                sync_local_metadata_to_datastore(
                    DATASTORE_LOCAL_DIR, self.task_datastore
                )

        try:
            self._save_logs_sidecar.terminate()
        except:
            # Best effort kill
            pass

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1
            )[0]
