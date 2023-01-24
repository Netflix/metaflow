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
