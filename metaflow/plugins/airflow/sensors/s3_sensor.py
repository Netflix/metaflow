from .base_sensor import AirflowSensorDecorator
from ..airflow_utils import SensorNames
from ..exception import AirflowException


class S3KeySensorDecorator(AirflowSensorDecorator):
    """
    The `@airflow_s3_key_sensor` decorator attaches a Airflow [S3KeySensor](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html#airflow.providers.amazon.aws.sensors.s3.S3KeySensor)
    before the start step of the flow. This decorator only works when a flow is scheduled on Airflow
    and is compiled using `airflow create`. More than one `@airflow_s3_key_sensor` can be
    added as a flow decorators. Adding more than one decorator will ensure that `start` step
    starts only after all sensors finish.

    Parameters
    ----------
    timeout : int
        Time, in seconds before the task times out and fails. (Default: 3600)
    poke_interval : int
        Time in seconds that the job should wait in between each try. (Default: 60)
    mode : str
        How the sensor operates. Options are: { poke | reschedule }. (Default: "poke")
    exponential_backoff : bool
        allow progressive longer waits between pokes by using exponential backoff algorithm. (Default: True)
    pool : str
        the slot pool this task should run in,
        slot pools are a way to limit concurrency for certain tasks. (Default:None)
    soft_fail : bool
        Set to true to mark the task as SKIPPED on failure. (Default: False)
    name : str
        Name of the sensor on Airflow
    description : str
        Description of sensor in the Airflow UI
    bucket_key : Union[str, List[str]]
        The key(s) being waited on. Supports full s3:// style url or relative path from root level.
        When it's specified as a full s3:// url, please leave `bucket_name` as None
    bucket_name : str
        Name of the S3 bucket. Only needed when bucket_key is not provided as a full s3:// url.
        When specified, all the keys passed to bucket_key refers to this bucket. (Default:None)
    wildcard_match : bool
        whether the bucket_key should be interpreted as a Unix wildcard pattern. (Default: False)
    aws_conn_id : str
        a reference to the s3 connection on Airflow. (Default: None)
    verify : bool
        Whether or not to verify SSL certificates for S3 connection. (Default: None)
    """

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

    def validate(self, flow):
        if self.attributes["bucket_key"] is None:
            raise AirflowException(
                "`bucket_key` for `@%s`cannot be empty." % (self.name)
            )
        super().validate(flow)
