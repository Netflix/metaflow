from ..exception import MetaflowException

class DataException(MetaflowException):
    headline = "Data store error"


class UnpicklizableArtifactException(MetaflowException):
    headline = "Cannot Picklize Artifact"

    def __init__(self,artifact_name):
        msg = 'Cannot pickle dump artifact named "%s"' % artifact_name
        super().__init__(msg=msg, lineno=None)