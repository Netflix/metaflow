from itertools import chain


class SpinInputsDatastore(object):
    def __init__(self, inp_datastores, artifacts={}):
        # This is a linear step, so we only have one input datastore
        self._inp_datastores = inp_datastores
        self._artifacts = artifacts
        self._step_name = self._inp_datastores.step_name

        # Set them to empty dictionaries in order to persist artifacts
        # See `persist` method in `TaskDatastore` for more details
        self._data = {}
        self._objects = {}
        self._info = {}

    def __contains__(self, name):
        try:
            _ = self.__getattr__(name)
        except AttributeError:
            return False
        return True

    def __getitem__(self, name):
        return self.__getattr__(name)

    def __setitem__(self, name, value):
        self._data[name] = value

    def __getattr__(self, name):
        # Check internal data first
        if name in self._data:
            return self._data[name]

        # We always look for any artifacts provided by the user first
        if name in self._artifacts:
            return self._artifacts[name]

        # If the user has not provided the artifact, we look for it in the
        # task using the client API
        try:
            return self._inp_datastores.get(name)
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self._step_name}`."
            )

    def get(self, key, default=None):
        try:
            return self.__getattr__(key)
        except AttributeError:
            return default
