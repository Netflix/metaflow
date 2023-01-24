from .external_task_sensor import ExternalTaskSensorDecorator
from .s3_sensor import S3KeySensorDecorator
from .sql_sensor import SQLSensorDecorator

SUPPORTED_SENSORS = [
    ExternalTaskSensorDecorator,
    S3KeySensorDecorator,
    SQLSensorDecorator,
]
