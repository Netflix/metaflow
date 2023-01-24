from .base_sensor import AirflowSensorDecorator
from ..airflow_utils import SensorNames
from ..exception import AirflowException


class S3KeySensorDecorator(AirflowSensorDecorator):
    name = "airflow_s3_key_sensor"
    operator_type = SensorNames.S3_SENSOR
    # Arg specification can be found here :
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html#airflow.providers.amazon.aws.sensors.s3.S3KeySensor
    defaults = dict(
        **AirflowSensorDecorator.defaults,
        bucket_key=None,  # Required
        bucket_name=None,
        wildcard_match=False,
        aws_conn_id=None,
        verify=None,  # `verify (Optional[Union[str, bool]])` Whether or not to verify SSL certificates for S3 connection.
        #  `verify` is a airflow variable.
    )

    def validate(self):
        if self.attributes["bucket_key"] is None:
            raise AirflowException(
                "`bucket_key` for `@%s`cannot be empty." % (self.name)
            )
        super().validate()
