from metaflow import step, FlowSpec
from payloads import EVENT_NAME, PURE_PAYLOADS, JSONSTR_PAYLOADS
import json


class TriggerArgoParamsTest(FlowSpec):
    @step
    def start(self):
        from metaflow.integrations import ArgoEvent

        # trigger events
        for idx, pl in enumerate(PURE_PAYLOADS):
            ArgoEvent(EVENT_NAME).publish({"payload_index": idx, **pl})

        # Test json serialized payload values.
        # start indexing correctly after the pure_payloads, which are first in the overall PAYLOADS
        for idx, pl in enumerate(JSONSTR_PAYLOADS, start=len(PURE_PAYLOADS)):
            serialized_pl = {k: json.dumps(v) for k, v in pl.items()}
            ArgoEvent(EVENT_NAME).publish({"payload_index": idx, **serialized_pl})

        self.next(self.end)

    @step
    def end(self):
        print("Done! 🏁")


if __name__ == "__main__":
    TriggerArgoParamsTest()
