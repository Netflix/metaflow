from baseflow import BaseParamsFlow
from metaflow import trigger, step, Parameter, catch
from payloads import EVENT_NAME, PAYLOADS


@trigger(event=EVENT_NAME)
class EventParamFlow(BaseParamsFlow):
    payload_index = Parameter(name="payload_index", default=None, type=int)

    @step
    def end(self):
        # we do some validation on the parameter values against known payloads and default values
        # just to be sure things are working as expected.
        print("Payload index is: %s" % self.payload_index)
        if self.payload_index is None:
            raise Exception(
                "payload index not provided, not possible to assert parameter validity."
            )

        params_dict = {
            k: getattr(self, k.replace("-", "_")) for k in self.param_defaults.keys()
        }
        print("Testing event against a known payload")
        pl = PAYLOADS[self.payload_index]  # pylint: disable=invalid-sequence-index

        for k, v in params_dict.items():
            if k in pl:
                # param value needs to be from payload if provided
                if v != pl[k]:
                    raise Exception(
                        f"Payload value does not match parameter value.\nParameter {k} has value {v} instead of {pl[k]} from the payload."
                    )
            else:
                # param value should be default if not provided in payload.
                if v != self.param_defaults[k]:
                    raise Exception(
                        f"Parameter {k} should have the default value {self.param_defaults[k]} instead of {v} as no value was provided in the payload"
                    )

        # Also raise if start step evals produced an exception that was caught
        test_failure = getattr(self, "test_failure", None)

        if test_failure is not None:
            raise test_failure


if __name__ == "__main__":
    EventParamFlow()
