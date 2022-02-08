import os
from metaflow import S3, current


def get_s3_logger_url(run):
    """
    Return URL for the logger directory (e.g for TensorBoard logger).
    """
    return os.path.join(S3(run=run)._s3root, "logger/")


def get_s3_checkpoint_url(run):
    """
    Return URL of the S3 prefix for checkpoints for the run.
    """
    return os.path.join(S3(run=run)._s3root, "checkpoints/")


def get_s3_latest_checkpoint_url(run):
    """
    Returns URL to the latest checkpoint, based on timestamp of files in the
    checkpoints/ prefix of the run's data.
    If there are no checkpoints, None is returned.
    """
    if current.origin_run_id is None:
        s3 = S3(run=run)
    else:
        s3 = S3(run=run, use_origin_run_id=True)
    checkpoints = s3.list_paths(["checkpoints/"])
    if not checkpoints:
        return None
    if len(checkpoints) == 1:
        return checkpoints[0].url
    with_infos = s3.info_many(["checkpoints/" + cpt.key for cpt in checkpoints])
    return max(with_infos, key=lambda info: info.last_modified).url
