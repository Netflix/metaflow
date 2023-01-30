from .base_sensor import AirflowSensorDecorator
from ..airflow_utils import SensorNames
from ..exception import AirflowException


class SQLSensorDecorator(AirflowSensorDecorator):
    """
    The `@airflow_sql_sensor` decorator attaches a Airflow [SqlSensor](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/sql/index.html#airflow.sensors.sql.SqlSensor)
    before the start step of the flow. This decorator only works when a flow is scheduled on Airflow
    and is compiled using `airflow create`. More than one `@airflow_sql_sensor` can be
    added as a flow decorators. Adding more than one decorator will ensure that `start` step
    starts only after all sensors finish.

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
    sql : str
        The sql to run. To pass, it needs to return at least one cell that
        contains a non-zero / empty string value.
    parameters : List[str] | Dict
        The parameters to render the SQL query with (optional). (Default: None)
    fail_on_empty : bool
        Explicitly fail on no rows returned. (Default: True)
    conn_id : string
        a reference to the SQL connection on Airflow.
    """

    name = "airflow_sql_sensor"
    operator_type = SensorNames.SQL_SENSOR
    # Arg specification can be found here :
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/sql/index.html#airflow.sensors.sql.SqlSensor
    defaults = dict(
        **AirflowSensorDecorator.defaults,
        conn_id=None,
        sql=None,
        # success = None, # sucess/failure require callables. Wont be supported at start since not serialization friendly.
        # failure = None,
        parameters=None,
        fail_on_empty=True,
    )

    def validate(self):
        if self.attributes["conn_id"] is None:
            raise AirflowException(
                "`%s` argument of `@%s`cannot be `None`." % ("conn_id", self.name)
            )
            raise _arg_exception("conn_id", self.name, None)
        if self.attributes["sql"] is None:
            raise AirflowException(
                "`%s` argument of `@%s`cannot be `None`." % ("sql", self.name)
            )
        super().validate()
