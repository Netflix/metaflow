from metaflow.exception import MetaflowException


class MetaflowGSPackageError(MetaflowException):
    headline = "Missing required packages google-cloud-storage google-auth"
