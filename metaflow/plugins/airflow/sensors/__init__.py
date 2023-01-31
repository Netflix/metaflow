from .external_task_sensor import ExternalTaskSensorDecorator
from .s3_sensor import S3KeySensorDecorator

SUPPORTED_SENSORS = [
    ExternalTaskSensorDecorator,
    S3KeySensorDecorator,
]
