from metaflow.exception import MetaflowException


class SnowflakeException(MetaflowException):
    headline = "Snowflake error"


class SnowparkException(MetaflowException):
    headline = "Snowpark error"


class SnowparkKilledException(MetaflowException):
    headline = "Snowpark job killed"
