import os
import tarfile
from typing import Dict
from urllib.parse import urlparse

from metaflow import current, util
from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.kfp.kfp_foreach_splits import KfpForEachSplits


class KfpInternalDecorator(StepDecorator):
    name = "kfp_internal"

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != "s3":
            raise Exception("The *@kfp_internal* decorator requires --datastore=s3.")

        self.datastore = datastore
        self.logger = logger

    def task_pre_step(
        self,
        step_name,
        datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
    ):
        """
        Analogous to step_functions_decorator.py
        Invoked from Task.run_step within the KFP container
        """
        # TODO: any other KFP environment variables to get and register to Metadata service?
        meta = {"kfp-execution": run_id}
        entries = [
            MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()
        ]

        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

        if metadata.TYPE == "local":
            self.ds_root = datastore.root
        else:
            self.ds_root = None

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        """
        Analogous to step_functions_decorator.py
        Invoked from Task.run_step within the KFP container
        """
        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            return
        else:
            # TODO: Could we copy [context file, metadata.tgz, stdout files] in
            #   parallel using the S3 client shaving off a few seconds for every
            #   task??  These seconds add up when running lightweight Metaflow
            #   tests on KFP.
            if self.ds_root:
                # We have a local metadata service so we need to persist it to the datastore.
                # Note that the datastore is *always* s3 (see runtime_task_created function)
                with util.TempDir() as td:
                    tar_file_path = os.path.join(td, "metadata.tgz")
                    with tarfile.open(tar_file_path, "w:gz") as tar:
                        # The local metadata is stored in the local datastore
                        # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
                        tar.add(DATASTORE_LOCAL_DIR)
                    # At this point we upload what need to s3
                    s3, _ = get_s3_client()
                    with open(tar_file_path, "rb") as f:
                        path = os.path.join(
                            self.ds_root,
                            MetaflowDataStore.filename_with_attempt_prefix(
                                "metadata.tgz", retry_count
                            ),
                        )
                        url = urlparse(path)
                        s3.upload_fileobj(f, url.netloc, url.path.lstrip("/"))
            else:
                # we are publishing to a Metadata service
                pass

            if graph[step_name].type == "foreach":
                # Save context to S3 for downstream DAG steps to access this
                # step's foreach_splits
                with KfpForEachSplits(
                    graph, step_name, current.run_id, self.datastore, self.logger
                ) as split_contexts:
                    foreach_splits: Dict = split_contexts.build_foreach_splits(flow)

                    # Pass along foreach_splits to step_op_func
                    KfpForEachSplits.save_foreach_splits_to_local_fs(foreach_splits)

                    # If this step is retried, then S3 doesn't guarantee
                    # consistency that the last uploaded data is returned. It'd
                    # be very rare that the data would change as this is the
                    # last step, and we assume the same inputs should produce
                    # the same outputs.
                    split_contexts.upload_foreach_splits_to_flow_root(foreach_splits)
