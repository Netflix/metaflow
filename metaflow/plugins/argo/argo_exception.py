from metaflow.exception import MetaflowException
from requests import HTTPError


class ArgoException(MetaflowException):
    headline = 'Argo Workflows error'

    def __init__(self, msg='', lineno=None):
        if isinstance(msg, HTTPError):
            msg = msg.response.json()['message']
        elif isinstance(msg, Exception):
            msg = str(msg)
        super().__init__(msg, lineno)
