from metaflow.exception import MetaflowInternalError

def only_if_not_closed(f):
    def method(self, *args, **kwargs):
        if self.is_closed:
            raise MetaflowInternalError(
                "Tried to access a datastore after closing it")
        return f(self, *args, **kwargs)
    return method