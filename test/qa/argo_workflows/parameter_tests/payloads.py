PURE_PAYLOADS = [
    {
        "param_a": "default value A",
        "param-b": ["a", "b"],
        "param-c": {"test": 1},
        "param-d": 123,
        "param-e": 1.23,
        "param-g": True,
        "param-opt": None,
        "param-opttwo": "null",
        "param-optthree": None,
    },  # default values but through payload.
    {
        "param_a": "custom payload A\nwith newlines\n but not trailing ones.",
        "param-b": ["C", "E"],
    },
    {
        "param_a": "http://example.com/test?a=123&b=test  and some values \"' '' && testing! \\",
        "param-b": [""],
        "param-g": False,
    },
    {"param_a": "Only supplied param_a"},
    {},  # Tests all default params
]
JSONSTR_PAYLOADS = [
    {
        "param-b": ["json", "serialized"],
        "param-c": {"json-serialized": 1},
    },  # meant to be json serialized before publishing
]

PAYLOADS = [*PURE_PAYLOADS, *JSONSTR_PAYLOADS]

EVENT_NAME = "params_test_event"
