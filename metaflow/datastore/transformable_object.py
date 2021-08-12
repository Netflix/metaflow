class TransformableObject(object):
    # Very simple wrapper class to only keep one transform
    # of an object. This is to force garbage collection
    # on the transformed object if the transformation is
    # successful
    def __init__(self, current_object):
        self._object = current_object
        self._original_type = type(self._object)

    def transform(self, transformer):
        # Transformer is a function taking one argument (the current object)
        # and returning anotherobject which will replace the current object
        # if transformer does not raise an exception
        try:
            temp = transformer(self._object)
            self._object = temp
        except:  # noqa E722
            raise

    @property
    def current(self):
        return self._object

    @property
    def current_type(self):
        return type(self._object)

    @property
    def original_type(self):
        return self._original_type