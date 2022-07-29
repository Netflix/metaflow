from metaflow.exception import MetaflowException


class AirflowException(MetaflowException):
    headline = "Airflow Exception"

    def __init__(self, msg):
        super().__init__(msg)


class NotSupportedException(MetaflowException):
    headline = "Not yet supported with Airflow"
