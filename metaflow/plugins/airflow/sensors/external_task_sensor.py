from .base_sensor import AirflowSensorDecorator
from ..airflow_utils import SensorNames
from ..exception import AirflowException
from datetime import timedelta


AIRFLOW_STATES = dict(
    QUEUED="queued",
    RUNNING="running",
    SUCCESS="success",
    SHUTDOWN="shutdown",  # External request to shut down,
    FAILED="failed",
    UP_FOR_RETRY="up_for_retry",
    UP_FOR_RESCHEDULE="up_for_reschedule",
    UPSTREAM_FAILED="upstream_failed",
    SKIPPED="skipped",
)


class ExternalTaskSensorDecorator(AirflowSensorDecorator):
    """
    The `@airflow_external_task_sensor` decorator attaches a Airflow [ExternalTaskSensor](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/external_task/index.html#airflow.sensors.external_task.ExternalTaskSensor) before the start step of the flow.
    This decorator only works when a flow is scheduled on Airflow and is compiled using `airflow create`. More than one `@airflow_external_task_sensor` can be added as a flow decorators. Adding more than one decorator will ensure that `start` step starts only after all sensors finish.

    Parameters
    ----------
    timeout : int
        Time, in seconds before the task times out and fails. (Default: 3600)
    poke_interval : int
        Time in seconds that the job should wait in between each try. (Default: 60)
    mode : string
        How the sensor operates. Options are: { poke | reschedule }. (Default: "poke")
    exponential_backoff : bool
        allow progressive longer waits between pokes by using exponential backoff algorithm. (Default: True)
    pool : string
        the slot pool this task should run in,
        slot pools are a way to limit concurrency for certain tasks. (Default:None)
    soft_fail : bool
        Set to true to mark the task as SKIPPED on failure. (Default: False)
    name : string
        Name of the sensor on Airflow
    description : string
        Description of sensor in the Airflow UI
    external_dag_id : string
        The dag_id that contains the task you want to wait for.
    external_task_ids : List[string]
        The list of task_ids that you want to wait for.
        If None (default value) the sensor waits for the DAG. (Default: None)
    allowed_states : List[string]
        Iterable of allowed states, (Default: ['success'])
    failed_states : List[string]
        Iterable of failed or dis-allowed states. (Default: None)
    execution_delta : datetime.timedelta
        time difference with the previous execution to look at,
        the default is the same logical date as the current task or DAG. (Default: None)
    check_existence: bool
        Set to True to check if the external task exists or check if
        the DAG to wait for exists. (Default: True)
    """

    operator_type = SensorNames.EXTERNAL_TASK_SENSOR
    # Docs:
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/external_task/index.html#airflow.sensors.external_task.ExternalTaskSensor
    name = "airflow_external_task_sensor"
    defaults = dict(
        **AirflowSensorDecorator.defaults,
        external_dag_id=None,
        external_task_ids=None,
        allowed_states=[AIRFLOW_STATES["SUCCESS"]],
        failed_states=None,
        execution_delta=None,
        check_existence=True,
        # We cannot add `execution_date_fn` as it requires a python callable.
        # Passing around a python callable is non-trivial since we are passing a
        # callable from metaflow-code to airflow python script. Since we cannot
        # transfer dependencies of the callable, we cannot gaurentee that the callable
        # behave exactly as the user expects
    )

    def serialize_operator_args(self):
        task_args = super().serialize_operator_args()
        if task_args["execution_delta"] is not None:
            task_args["execution_delta"] = dict(
                seconds=task_args["execution_delta"].total_seconds()
            )
        return task_args

    def validate(self):
        if self.attributes["external_dag_id"] is None:
            raise AirflowException(
                "`%s` argument of `@%s`cannot be `None`."
                % ("external_dag_id", self.name)
            )

        if type(self.attributes["allowed_states"]) == str:
            if self.attributes["allowed_states"] not in list(AIRFLOW_STATES.values()):
                raise AirflowException(
                    "`%s` is an invalid input of `%s` for `@%s`. Accepted values are %s"
                    % (
                        str(self.attributes["allowed_states"]),
                        "allowed_states",
                        self.name,
                        ", ".join(list(AIRFLOW_STATES.values())),
                    )
                )
        elif type(self.attributes["allowed_states"]) == list:
            enum_not_matched = [
                x
                for x in self.attributes["allowed_states"]
                if x not in list(AIRFLOW_STATES.values())
            ]
            if len(enum_not_matched) > 0:
                raise AirflowException(
                    "`%s` is an invalid input of `%s` for `@%s`. Accepted values are %s"
                    % (
                        str(" OR ".join(["'%s'" % i for i in enum_not_matched])),
                        "allowed_states",
                        self.name,
                        ", ".join(list(AIRFLOW_STATES.values())),
                    )
                )
        else:
            self.attributes["allowed_states"] = [AIRFLOW_STATES["SUCCESS"]]

        if self.attributes["execution_delta"] is not None:
            if not isinstance(self.attributes["execution_delta"], timedelta):
                raise AirflowException(
                    "`%s` is an invalid input type of `execution_delta` for `@%s`. Accepted type is `datetime.timedelta`"
                    % (
                        str(type(self.attributes["execution_delta"])),
                        self.name,
                    )
                )
        super().validate()
