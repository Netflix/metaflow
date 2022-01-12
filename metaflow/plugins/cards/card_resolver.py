from collections import namedtuple

from .card_datastore import CardDatastore


def _chase_origin(task):
    from metaflow.client import Task

    task_origin = None
    ref_task = task
    while ref_task.origin_pathspec is not None:
        task_origin = ref_task.origin_pathspec
        ref_task = Task(task_origin)
    return task_origin


def resumed_info(task):
    return _chase_origin(task)


def resolve_paths_from_task(
    flow_datastore,
    pathspec=None,
    type=None,
    hash=None,
    card_id=None,
):
    card_datastore = CardDatastore(flow_datastore, pathspec=pathspec)
    card_paths_found = card_datastore.extract_card_paths(
        card_type=type, card_hash=hash, card_id=card_id
    )
    return card_paths_found, card_datastore
