from metaflow.exception import MetaflowException


class SlurmException(MetaflowException):
    headline = "Slurm error"


class SlurmKilledException(MetaflowException):
    headline = "Slurm job killed"
