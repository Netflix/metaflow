from unittest.mock import MagicMock, patch

import pytest

from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator


def test_priority_class_in_defaults():
    """priority_class key exists in KubernetesDecorator.defaults."""
    assert "priority_class" in KubernetesDecorator.defaults


def test_priority_class_default_from_config():
    """Default value comes from KUBERNETES_PRIORITY_CLASS config variable."""
    from metaflow.metaflow_config import KUBERNETES_PRIORITY_CLASS

    assert KubernetesDecorator.defaults["priority_class"] == KUBERNETES_PRIORITY_CLASS


def test_priority_class_attribute_set_via_decorator():
    """Explicit priority_class value is preserved in attributes."""
    deco = KubernetesDecorator(attributes={"priority_class": "high-priority"})
    assert deco.attributes["priority_class"] == "high-priority"


def test_priority_class_flows_to_pod_spec():
    """priority_class kwarg becomes priorityClassName in V1PodSpec."""
    from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

    mock_client = MagicMock()
    # The client.get() returns the kubernetes.client module mock
    k8s_module = MagicMock()
    mock_client.get.return_value = k8s_module

    kwargs = dict(
        use_tmpfs=False,
        tmpfs_size=None,
        tmpfs_path="/metaflow_temp",
        shared_memory=None,
        qos="Burstable",
        cpu="1",
        memory="4096",
        disk="10240",
        gpu=None,
        gpu_vendor="nvidia",
        image="python:3.9",
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[],
        command=["echo", "hello"],
        step_name="my_step",
        namespace="default",
        timeout_in_seconds=300,
        annotations={},
        labels={},
        port=None,
        secrets=[],
        node_selector=None,
        tolerations=[],
        persistent_volume_claims=None,
        priority_class="batch-high",
        service_account="default",
        security_context={},
    )
    job = KubernetesJob(client=mock_client, **kwargs)
    job.create_job_spec()

    # V1PodSpec should have been called with priority_class_name="batch-high"
    k8s_module.V1PodSpec.assert_called_once()
    call_kwargs = k8s_module.V1PodSpec.call_args
    assert call_kwargs[1]["priority_class_name"] == "batch-high"


def test_priority_class_none_passes_none_to_pod_spec():
    """When priority_class is None, priorityClassName is None in V1PodSpec."""
    from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

    mock_client = MagicMock()
    k8s_module = MagicMock()
    mock_client.get.return_value = k8s_module

    kwargs = dict(
        use_tmpfs=False,
        tmpfs_size=None,
        tmpfs_path="/metaflow_temp",
        shared_memory=None,
        qos="Burstable",
        cpu="1",
        memory="4096",
        disk="10240",
        gpu=None,
        gpu_vendor="nvidia",
        image="python:3.9",
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[],
        command=["echo", "hello"],
        step_name="my_step",
        namespace="default",
        timeout_in_seconds=300,
        annotations={},
        labels={},
        port=None,
        secrets=[],
        node_selector=None,
        tolerations=[],
        persistent_volume_claims=None,
        priority_class=None,
        service_account="default",
        security_context={},
    )
    job = KubernetesJob(client=mock_client, **kwargs)
    job.create_job_spec()

    call_kwargs = k8s_module.V1PodSpec.call_args
    assert call_kwargs[1]["priority_class_name"] is None
