from ..exception import MetaflowException

class DataException(MetaflowException):
    headline = "Data store error"


class ArtifactTooLarge(object):
    def __str__(self):
        return '< artifact too large >'