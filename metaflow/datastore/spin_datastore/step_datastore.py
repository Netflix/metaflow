from itertools import chain


class SpinStepDatastore(object):
    def __init__(self, inp_datastores, _foreach_stack, artifacts={}):
        # This is a linear step, so we only have one input datastore
        self._inp_datastores = inp_datastores[0]
        self._foreach_stack = _foreach_stack
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

        # If the linear step is part of a foreach step, we need to set the input attribute
        # and the index attribute
        if name == "input":
            if len(self._foreach_stack) == 0:
                raise AttributeError(
                    f"Attribute '{name}' does not exist for step `{self._step_name}` as it is not part of "
                    f"a foreach step."
                )
            # input only exists for steps immediately after a foreach split
            # we check for that by comparing the length of the foreach-step-names
            # attribute of the task and its immediate ancestors
            foreach_step = self._foreach_stack[-1].step
            if self._step_name != foreach_step:
                return None  # input does not exist, so we return None

            foreach_index = self._foreach_stack[-1].index
            foreach_var = self._foreach_stack[-1].var

            # Fetch the artifact corresponding to the foreach var and index from the previous task
            input_val = self._inp_datastores[foreach_var].data[foreach_index]
            setattr(self, name, input_val)
            return input_val

        # If the linear step is part of a foreach step, we need to set the index attribute
        if name == "index":
            if len(self._foreach_stack) == 0:
                raise AttributeError(
                    f"Attribute '{name}' does not exist for step `{self.step_name}` as it is not part of a "
                    f"foreach step."
                )

            foreach_index = self._foreach_stack[-1].index
            setattr(self, name, foreach_index)
            return foreach_index

        # If the user has not provided the artifact, we look for it in the
        # task using the client API
        try:
            return getattr(self.previous_task.artifacts, name).data
        except AttributeError:
            raise AttributeError(
                f"Attribute '{name}' not found in the previous execution of the task for "
                f"`{self.step_name}`."
            )

    def get(self, key, default=None):
        try:
            return self.__getattr__(key)
        except AttributeError:
            return default
