from metaflow.exception import MetaflowException


class MetaflowAzureAuthenticationError(MetaflowException):
    headline = "Failed to authenticate with Azure"


class MetaflowAzureResourceError(MetaflowException):
    headline = "Failed to access Azure resource"
