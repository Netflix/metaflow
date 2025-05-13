from metaflow import Run


def _get_artifact():
    task = Run("ComplexDAGFlow/2")["step_d"].task
    task_pathspec = next(task.parent_task_pathspecs)
    _, inp_path = task_pathspec.split("/", 1)
    return {inp_path: {"my_output": [-1]}}


ARTIFACTS = _get_artifact()
