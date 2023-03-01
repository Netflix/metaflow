import urllib3
import json


class ArgoEvent(object):
    def __init__(self, name, payload={}):
        # TODO: Add guardrails for name if any
        self.name = name
        self._payload = payload

    def add_to_payload(self, key, value):
        self._payload[key] = str(value)
        return self

    def publish(self, payload={}, force=False):
        # TODO: Emit only iff force or running on Argo Workflows
        try:
            # Handle scenarios where the URL is incorrect. Currently it hangs around
            url = "http://10.10.29.11:12000/event"
            request = urllib3.PoolManager().request(
                "POST",
                url,
                headers={"Content-Type": "application/json"},
                body=json.dumps(
                    {
                        # TODO: Ensure this schema is backwards compatible
                        "name": self.name,
                        "payload": {**self._payload, **payload},
                    }
                ),
                timeout=30.0,  # should be enough - still hangs though :(
            )
            # TODO: log the fact that event has been emitted
            # TODO: should these logs be in mflogs or just orchestrator logs??
            # TODO: what should happen if event can't be emitted
            print(request.data)
        except Exception as e:
            print("Encountered excpetion while emitting argo event: %" % repr(e))
