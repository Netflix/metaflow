import json
import os
import tarfile
import tempfile
from urllib.parse import urlparse

from metaflow import util, current
from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.decorators import StepDecorator
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR


class KfpInternalDecorator(StepDecorator):
    name = "kfp_internal"

    def task_pre_step(
        self,
        step_name,
        ds,
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
        """
        # TODO: any other KFP environment variables to get and register to Metadata service?
        meta = {"kfp-execution": os.environ["METAFLOW_RUN_ID"]}
        entries = [MetaDatum(field=k, value=v, type=k) for k, v in meta.items()]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

        if metadata.TYPE == "local":
            self.ds_root = ds.root
        else:
            self.ds_root = None

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        """
        Analogous to step_functions_decorator.py
        """
        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            return

        # For foreaches, we need to dump the cardinality of the fanout
        # for the KFP parallelFor
        out_dict = dict(step_name=step_name, task_id=current.task_id)
        if graph[step_name].type == "foreach":
            out_dict["foreach_num_splits"] = flow._foreach_num_splits
            next_task_id = current.task_id + 1
            out_dict["split_indexes"] = [
                i for i in range(next_task_id, next_task_id + flow._foreach_num_splits)
            ]

        with open(
            os.path.join(tempfile.gettempdir(), "kfp_metaflow_out_dict.json"), "w"
        ) as file:
            json.dump(out_dict, file)

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
