from .base_sensor import AirflowSensorDecorator
from ..airflow_utils import SensorNames
from ..exception import AirflowException


class SQLSensorDecorator(AirflowSensorDecorator):
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
