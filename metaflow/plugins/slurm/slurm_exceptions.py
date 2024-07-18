from metaflow.exception import MetaflowException


class SlurmException(MetaflowException):
    headline = "Slurm error"
