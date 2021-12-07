from collections import namedtuple

from .card_datastore import CardDatastore

ResumedInfo = namedtuple("ResumedInfo", ["task_resumed", "origin_task_pathspec"])


def _chase_origin(task):
    from metaflow.client import Task

    task_origin = None
    ref_task = task
    while ref_task.origin_pathspec is not None:
        task_origin = ref_task.origin_pathspec
        ref_task = Task(task_origin)
    return task_origin


def resumed_info(task):
    origin_pathspec = _chase_origin(task)
    return ResumedInfo(origin_pathspec is not None, origin_pathspec)


def resolve_paths_from_task(
    flow_datastore,
    run_id,
    step_name,
    task_id,
    pathspec=None,
    type=None,
    hash=None,
):
    card_datastore = CardDatastore(
        flow_datastore, run_id, step_name, task_id, path_spec=pathspec
    )
    card_paths_found = card_datastore.extract_card_paths(card_type=type, card_hash=hash)
    return card_paths_found, card_datastore
