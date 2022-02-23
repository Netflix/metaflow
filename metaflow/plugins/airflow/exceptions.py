from metaflow.exception import MetaflowException


class AirflowNotPresent(MetaflowException):
    headline = "Airflow cannot be imported"

    def __init__(self):
        msg = "Airflow dependency is missing. Please install airflow to use the airflow cli command"
        super().__init__(msg)


class AirflowException(MetaflowException):
    headline = "Airflow Exception"

    def __init__(self, msg):
        super().__init__(msg)
