from metaflow import step, FlowSpec, Parameter, JSONType, catch


class BaseParamsFlow(FlowSpec):
    param_a = Parameter(name="param_a", default="default value A", type=str)

    param_b = Parameter(name="param-b", default=["a", "b"], type=JSONType)

    param_c = Parameter(name="param-c", default={"test": 1}, type=JSONType)

    param_d = Parameter(name="param-d", default=123, type=int)

    param_e = Parameter(name="param-e", default=1.23, type=float)

    param_f = Parameter(
        name="param-f", default='{"a": 123}', type=JSONType
    )  # for testing json serialization from string defaults

    param_g = Parameter(name="param-g", default=True, type=bool)

    param_opt = Parameter(name="param-opt", required=False)
    param_opttwo = Parameter(name="param-opttwo", default="null")
    param_optthree = Parameter(name="param-optthree", default=None)
    # bookkeeping to make testing easier. these match the parameter names.
    param_defaults = {
        "param_a": "default value A",
        "param-b": ["a", "b"],
        "param-c": {"test": 1},
        "param-d": 123,
        "param-e": 1.23,
        "param-f": {"a": 123},
        "param-g": True,
        "param-opt": None,
        "param-opttwo": "null",
        "param-optthree": None,
    }

    @catch(var="test_failure")
    @step
    def start(self):
        print("Starting 👋")
        # printing out values for debugging
        for k, v in self.param_defaults.items():
            print(
                f'{k.upper().replace("_", " ").replace("-", " ")}: {getattr(self, k.replace("-", "_"))}'
            )

        # check types of parameters
        for k, v in self.param_defaults.items():
            param_value = getattr(self, k.replace("-", "_"))
            if type(param_value) != type(v):
                raise Exception(
                    f"parameter {k} value is of the wrong type. Expected {type(v)} but is {type(param_value)}"
                )

        self.next(self.end)

    @step
    def end(self):
        print("Done! 🏁")
        # check for errors and raise
        test_failure = getattr(self, "test_failure", None)

        if test_failure is not None:
            raise test_failure
