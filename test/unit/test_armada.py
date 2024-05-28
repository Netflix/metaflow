import pytest

from metaflow.plugins.armada.armada import create_armada_pod_spec


def test_armada_create_armada_pod_spec():
    pod_spec = create_armada_pod_spec(["sleep 10"], {"test": "value"}, [])
    assert pod_spec is not None
    assert pod_spec[0].pod_spec.containers[0].args == ["sleep 10"]
    print(len(pod_spec))
    print(pod_spec)
    # assert False
