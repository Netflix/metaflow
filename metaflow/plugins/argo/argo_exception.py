from metaflow.exception import MetaflowException


class ArgoException(MetaflowException):
    headline = 'Argo Workflows error'
