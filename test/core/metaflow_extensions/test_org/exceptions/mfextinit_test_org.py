from metaflow.exception import MetaflowException


class MetaflowTestException(MetaflowException):
    headline = "Subservice error"

    def __init__(self, error):
        msg = "Test error: '%s'" % error
        super(MetaflowTestException, self).__init__(msg)
