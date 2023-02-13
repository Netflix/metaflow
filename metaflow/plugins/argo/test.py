import requests


class ArgoEventsClient(object):

    _payload = {}

    @classmethod
    def add_to_event(cls, key, value):
        cls._payload[key] = str(value)

    @classmethod
    def emit_event(cls, name, params):
        resp = requests.post(
            url, headers={"content-type": "application/json"}, json=body
        )
        resp.raise_for_status()
