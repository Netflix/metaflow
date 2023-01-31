import logging
import os
from typing import List, NamedTuple, Optional, TypeVar, Union
from urllib.parse import urlparse

from metaflow.current import current
from metaflow.datatools import S3
from metaflow.flowspec import FlowSpec
from metaflow.client.core import Run
from metaflow.datatools.s3 import S3Object

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())
_logger.setLevel(logging.INFO)

T = TypeVar("T")


def _get_s3_latest_checkpoint_s3_info(s3_checkpoint_dir: str) -> Optional[S3Object]:
    s3 = S3(s3root=s3_checkpoint_dir)
    checkpoints = s3.list_paths([""])
    if not checkpoints:
        return None

    with_infos: List = s3.info_many([cpt.key for cpt in checkpoints])
    return max(with_infos, key=lambda info: info.last_modified)


def _get_latest_checkpoint_name(checkpoint_dir: str) -> Optional[str]:
    if urlparse(checkpoint_dir).scheme == "s3":
        latest = _get_s3_latest_checkpoint_s3_info(checkpoint_dir)
        return latest.key if latest else None
    else:
        if os.path.exists(checkpoint_dir):
            files = os.listdir(checkpoint_dir)
            paths = [os.path.join(checkpoint_dir, basename) for basename in files]
            paths = [path for path in paths if os.path.isfile(path)]
            return max(paths, key=os.path.getctime)
        else:
            return None


def _get_checkpoint_dir(
    run: Optional[Union[FlowSpec, Run]] = None,
    step_name: Optional[str] = None,
    task_id: Optional[str] = None,
) -> str:
    temp_step_name = step_name if step_name else current.step_name
    temp_task_id = task_id if task_id else current.task_id
    return os.path.join(
        _get_run_root(run), f"checkpoints/{temp_step_name}/{temp_task_id}"
    )


def _get_run_root(run: Optional[Union[FlowSpec, Run]] = None) -> str:
    """Function separated out to unit test mock"""
    root_env = os.environ.get("CHECKPOINT_DIR")
    if root_env:
        return root_env
    else:
        temp_run = run if run else current.flow
        return S3(run=temp_run)._s3root


def _get_resume_checkpoint_path(
    checkpoint_dir: str,
    resume_checkpoint_path: Optional[str] = None,
) -> Optional[str]:
    if current.retry_count == 0:
        return resume_checkpoint_path
    elif current.retry_count > 0:
        key = _get_latest_checkpoint_name(checkpoint_dir)
        if key:
            ret = os.path.join(checkpoint_dir, key)
            _logger.debug(f"_get_resume_checkpoint_path returning {ret}")
            return ret
        else:
            _logger.info(
                f"{current.retry_count=} but using {resume_checkpoint_path=} because no checkpoint found."
            )
            return resume_checkpoint_path


CheckpointPaths = NamedTuple(
    "CheckpointPaths",
    [("checkpoint_dir", str), ("previous_checkpoint_path", Optional[str])],
)


def get_checkpoint_paths(
    resume_checkpoint_path: Optional[str] = None,
) -> CheckpointPaths:
    """
    This function gets the checkpoint_dir and resume path for a Flow Run step.

    Checkpointing allows a long running process to resume upon network,
    infrastructure, or SPOT interruptions or failures.  This is especially
    useful for expensive training or compute.

    The environment variable CHECKPOINT_DIR can override the checkpoint_dir path,
    which is useful for local or CICD runs not on S3.

    Args:
        resume_checkpoint_path (Optional[str], optional):
            If provided, this would be the initial CheckpointPaths.previous_checkpoint_path
            returned upon the first attempt, yet not on retries.
            Defaults to None, upon which it returns None on the first attempt.

    Returns:
        CheckpointPaths tuple of the following:
        - checkpoint_dir: Checkpoint S3 root path for this run and step.
        - previous_checkpoint_path: S3 path to the latest checkpoint under the checkpoint_dir, to resume from.
            This can be None on the first attempt when resume_checkpoint_path is None.

    Examples:
        1. The previous_checkpoint_path is None on the first attempt::

            @interruptible
            @retry
            @step
            def train(self):
                checkpoint_dir, previous_checkpoint_path = get_checkpoint_paths()
                ...

        2. `previous_checkpoint_path` is the latest file in the path on a second attempt:
        (s3://.../checkpoints/step_name/task_id/, s3://.../checkpoints/step_name/task_id/checkpoint1.pt)

        3. Example of resuming a checkpoint from a previous training run.
            `previous_checkpoint_path` is `self.resume_checkpoint_path` on the first attempt
            and is latest checkpoint path on subsequent attempts::

            @interruptible
            @retry
            @step
            def train(self):
                checkpoint_dir, previous_checkpoint_path = get_checkpoint_paths(self.resume_checkpoint_path)
                ...

        (s3://.../checkpoints/step_name/task_id/, s3://.../previous_run_checkpoint_path.pt)
    """
    checkpoint_dir: str = _get_checkpoint_dir()
    return CheckpointPaths(
        checkpoint_dir=checkpoint_dir,
        previous_checkpoint_path=_get_resume_checkpoint_path(
            checkpoint_dir, resume_checkpoint_path
        ),
    )
