CONTROL_TASK_TAG = "control_task"
CONTROL_AND_MAPPER_TAG = "control_and_mapper"  # Set if the task is both a control task and a vanilla UBF task.
UBF_CONTROL = "ubf_control"
UBF_TASK = "ubf_task"


class UnboundedForeachInput(object):
    """
    Plugins that wish to support `UnboundedForeach` need their special
    input(s) subclass this class.
    This is used by the runtime to detect the difference between bounded
    and unbounded foreach, based on the variable passed to `foreach`.
    """
