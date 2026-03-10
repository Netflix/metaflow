"""
Shared utilities for resume support across orchestrators (Argo, SFN).

These helpers enable the "in-step clone-or-run" pattern where each step
container decides at runtime whether to clone from the origin run or
execute normally.
"""

from ..datastore.datastore_set import TaskDataStoreSet


def compute_steps_to_rerun(graph, step_to_rerun=None):
    """
    Compute the set of steps that should be re-executed during a resume.

    If step_to_rerun is specified, returns that step plus all downstream steps.
    If step_to_rerun is None, returns an empty set (meaning: only rerun steps
    whose origin task was not successful).

    Parameters
    ----------
    graph : FlowGraph
        The flow's DAG graph.
    step_to_rerun : str, optional
        Name of the step from which to rerun.

    Returns
    -------
    set
        Set of step names that should be re-executed.
    """
    if step_to_rerun is None:
        return set()

    steps = {step_to_rerun}
    for step_name in graph.sorted_nodes:
        if step_name in steps:
            out_funcs = graph[step_name].out_funcs or []
            for next_step in out_funcs:
                steps.add(next_step)
    return steps


def parse_steps_to_rerun(steps_str):
    """
    Parse the comma-separated string of steps to rerun.

    Parameters
    ----------
    steps_str : str
        Comma-separated step names, or empty string.

    Returns
    -------
    set
        Set of step names.
    """
    if not steps_str:
        return set()
    return set(s.strip() for s in steps_str.split(",") if s.strip())


def find_origin_task(
    flow_datastore,
    graph,
    origin_run_id,
    step_name,
    current_run_id,
    input_paths,
    split_index,
):
    """
    Find the corresponding successful task in the origin run.

    Uses the same foreach index matching logic as NativeRuntime._find_origin_task,
    adapted for the scheduler context where each container independently looks up
    its origin task.

    Parameters
    ----------
    flow_datastore : FlowDataStore
        The flow's datastore.
    graph : FlowGraph
        The flow's DAG graph.
    origin_run_id : str
        Run ID of the origin run.
    step_name : str
        Name of the current step.
    current_run_id : str
        Run ID of the current (new) run.
    input_paths : list
        Pathspecs of input tasks (from parent steps in the current run).
    split_index : int or None
        Foreach split index, if applicable.

    Returns
    -------
    str or None
        The origin task pathspec (run_id/step_name/task_id) to clone from,
        or None if the step should execute normally.
    """
    # Load all origin tasks for this step
    origin_ds_set = TaskDataStoreSet(
        flow_datastore,
        origin_run_id,
        steps=[step_name],
        prefetch_data_artifacts=["_task_ok", "_foreach_stack"],
    )

    if step_name == "_parameters":
        origin = origin_ds_set.get_with_pathspec_index(
            "%s/_parameters[]" % origin_run_id
        )
        if origin is None:
            return None
        # _parameters is always considered successful
        return origin.pathspec

    node = graph[step_name]

    if not node.is_inside_foreach:
        # Non-foreach step: single task, look up with empty index
        origin = origin_ds_set.get_with_pathspec_index(
            "%s/%s[]" % (origin_run_id, step_name)
        )
    else:
        # Foreach step: compute index from parent's _foreach_stack
        if not input_paths:
            return None

        parent_pathspec = input_paths[0]
        parts = parent_pathspec.split("/")
        parent_step = parts[1]
        parent_task_id = parts[2]

        try:
            parent_ds = flow_datastore.get_task_datastore(
                current_run_id, parent_step, parent_task_id
            )
            foreach_stack = parent_ds["_foreach_stack"]
        except Exception:
            # If we can't load parent (e.g., it hasn't been cloned yet),
            # fall back to running the step normally
            return None

        # Determine join type for index computation
        is_foreach_join = node.type == "join" and any(
            graph[p].type == "foreach" for p in node.in_funcs
        )

        if is_foreach_join:
            # foreach-join pops the topmost index
            index = ",".join(str(s.index) for s in foreach_stack[:-1])
        elif split_index is not None:
            # foreach-split pushes a new index
            index = ",".join([str(s.index) for s in foreach_stack] + [str(split_index)])
        else:
            # linear step inside foreach: keep parent's index
            index = ",".join(str(s.index) for s in foreach_stack)

        origin = origin_ds_set.get_with_pathspec_index(
            "%s/%s[%s]" % (origin_run_id, step_name, index)
        )

    if origin is None:
        return None

    # Only clone if the origin task succeeded
    if origin["_task_ok"]:
        return origin.pathspec
    return None
