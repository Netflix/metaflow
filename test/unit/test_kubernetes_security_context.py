"""Tests for Kubernetes security context support (container and pod level)."""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_kubernetes_client():
    """Create a mock Kubernetes client that tracks calls to V1SecurityContext and V1PodSecurityContext."""
    with patch("metaflow.plugins.kubernetes.kubernetes_job.KubernetesJob") as _:
        from kubernetes import client

        yield client


class TestContainerSecurityContext:
    """Tests for container-level security context in KubernetesJob."""

    def test_security_context_applied_to_container(self):
        """Verify that security_context dict is passed to V1SecurityContext."""
        from unittest.mock import MagicMock

        mock_client_wrapper = MagicMock()
        from kubernetes import client as real_client

        mock_client_wrapper.get.return_value = real_client

        from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

        job = KubernetesJob(
            client=mock_client_wrapper,
            step_name="test_step",
            command=["echo", "hello"],
            namespace="default",
            service_account="default",
            image="python:3.9",
            image_pull_policy="Always",
            image_pull_secrets=[],
            cpu="1",
            memory="4096",
            disk="10240",
            gpu=None,
            gpu_vendor="nvidia",
            timeout_in_seconds=300,
            retries=0,
            port=None,
            use_tmpfs=False,
            tmpfs_size=None,
            tmpfs_path="/metaflow_temp",
            persistent_volume_claims=None,
            shared_memory=None,
            tolerations=[],
            labels={},
            annotations={},
            qos="Burstable",
            security_context={"run_as_user": 1000, "run_as_non_root": True},
            pod_security_context=None,
        )

        spec = job.create_job_spec()
        container = spec.template.spec.containers[0]
        assert container.security_context is not None
        assert container.security_context.run_as_user == 1000
        assert container.security_context.run_as_non_root is True

    def test_empty_security_context_not_applied(self):
        """Verify that an empty security_context does not set anything."""
        from unittest.mock import MagicMock

        mock_client_wrapper = MagicMock()
        from kubernetes import client as real_client

        mock_client_wrapper.get.return_value = real_client

        from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

        job = KubernetesJob(
            client=mock_client_wrapper,
            step_name="test_step",
            command=["echo", "hello"],
            namespace="default",
            service_account="default",
            image="python:3.9",
            image_pull_policy="Always",
            image_pull_secrets=[],
            cpu="1",
            memory="4096",
            disk="10240",
            gpu=None,
            gpu_vendor="nvidia",
            timeout_in_seconds=300,
            retries=0,
            port=None,
            use_tmpfs=False,
            tmpfs_size=None,
            tmpfs_path="/metaflow_temp",
            persistent_volume_claims=None,
            shared_memory=None,
            tolerations=[],
            labels={},
            annotations={},
            qos="Burstable",
            security_context=None,
            pod_security_context=None,
        )

        spec = job.create_job_spec()
        container = spec.template.spec.containers[0]
        assert container.security_context is None

    def test_security_context_with_capabilities(self):
        """Verify that capabilities can be set in security_context."""
        from unittest.mock import MagicMock
        from kubernetes import client as real_client

        mock_client_wrapper = MagicMock()
        mock_client_wrapper.get.return_value = real_client

        from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

        caps = real_client.V1Capabilities(drop=["ALL"], add=["NET_BIND_SERVICE"])
        job = KubernetesJob(
            client=mock_client_wrapper,
            step_name="test_step",
            command=["echo", "hello"],
            namespace="default",
            service_account="default",
            image="python:3.9",
            image_pull_policy="Always",
            image_pull_secrets=[],
            cpu="1",
            memory="4096",
            disk="10240",
            gpu=None,
            gpu_vendor="nvidia",
            timeout_in_seconds=300,
            retries=0,
            port=None,
            use_tmpfs=False,
            tmpfs_size=None,
            tmpfs_path="/metaflow_temp",
            persistent_volume_claims=None,
            shared_memory=None,
            tolerations=[],
            labels={},
            annotations={},
            qos="Burstable",
            security_context={
                "capabilities": caps,
                "read_only_root_filesystem": True,
            },
            pod_security_context=None,
        )

        spec = job.create_job_spec()
        container = spec.template.spec.containers[0]
        assert container.security_context.read_only_root_filesystem is True
        assert container.security_context.capabilities is not None


class TestPodSecurityContext:
    """Tests for pod-level security context in KubernetesJob."""

    def test_pod_security_context_applied(self):
        """Verify that pod_security_context dict is passed to V1PodSecurityContext."""
        from unittest.mock import MagicMock
        from kubernetes import client as real_client

        mock_client_wrapper = MagicMock()
        mock_client_wrapper.get.return_value = real_client

        from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

        job = KubernetesJob(
            client=mock_client_wrapper,
            step_name="test_step",
            command=["echo", "hello"],
            namespace="default",
            service_account="default",
            image="python:3.9",
            image_pull_policy="Always",
            image_pull_secrets=[],
            cpu="1",
            memory="4096",
            disk="10240",
            gpu=None,
            gpu_vendor="nvidia",
            timeout_in_seconds=300,
            retries=0,
            port=None,
            use_tmpfs=False,
            tmpfs_size=None,
            tmpfs_path="/metaflow_temp",
            persistent_volume_claims=None,
            shared_memory=None,
            tolerations=[],
            labels={},
            annotations={},
            qos="Burstable",
            security_context=None,
            pod_security_context={"fs_group": 2000, "run_as_non_root": True},
        )

        spec = job.create_job_spec()
        pod_spec = spec.template.spec
        assert pod_spec.security_context is not None
        assert pod_spec.security_context.fs_group == 2000
        assert pod_spec.security_context.run_as_non_root is True

    def test_empty_pod_security_context_not_applied(self):
        """Verify that empty pod_security_context does not set anything."""
        from unittest.mock import MagicMock
        from kubernetes import client as real_client

        mock_client_wrapper = MagicMock()
        mock_client_wrapper.get.return_value = real_client

        from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

        job = KubernetesJob(
            client=mock_client_wrapper,
            step_name="test_step",
            command=["echo", "hello"],
            namespace="default",
            service_account="default",
            image="python:3.9",
            image_pull_policy="Always",
            image_pull_secrets=[],
            cpu="1",
            memory="4096",
            disk="10240",
            gpu=None,
            gpu_vendor="nvidia",
            timeout_in_seconds=300,
            retries=0,
            port=None,
            use_tmpfs=False,
            tmpfs_size=None,
            tmpfs_path="/metaflow_temp",
            persistent_volume_claims=None,
            shared_memory=None,
            tolerations=[],
            labels={},
            annotations={},
            qos="Burstable",
            security_context=None,
            pod_security_context=None,
        )

        spec = job.create_job_spec()
        pod_spec = spec.template.spec
        assert pod_spec.security_context is None

    def test_both_security_contexts_applied(self):
        """Verify that both container and pod security contexts can be set simultaneously."""
        from unittest.mock import MagicMock
        from kubernetes import client as real_client

        mock_client_wrapper = MagicMock()
        mock_client_wrapper.get.return_value = real_client

        from metaflow.plugins.kubernetes.kubernetes_job import KubernetesJob

        job = KubernetesJob(
            client=mock_client_wrapper,
            step_name="test_step",
            command=["echo", "hello"],
            namespace="default",
            service_account="default",
            image="python:3.9",
            image_pull_policy="Always",
            image_pull_secrets=[],
            cpu="1",
            memory="4096",
            disk="10240",
            gpu=None,
            gpu_vendor="nvidia",
            timeout_in_seconds=300,
            retries=0,
            port=None,
            use_tmpfs=False,
            tmpfs_size=None,
            tmpfs_path="/metaflow_temp",
            persistent_volume_claims=None,
            shared_memory=None,
            tolerations=[],
            labels={},
            annotations={},
            qos="Burstable",
            security_context={"run_as_user": 1000, "allow_privilege_escalation": False},
            pod_security_context={
                "fs_group": 2000,
                "run_as_group": 3000,
                "supplemental_groups": [4000],
            },
        )

        spec = job.create_job_spec()

        # Check container-level
        container = spec.template.spec.containers[0]
        assert container.security_context.run_as_user == 1000
        assert container.security_context.allow_privilege_escalation is False

        # Check pod-level
        pod_spec = spec.template.spec
        assert pod_spec.security_context.fs_group == 2000
        assert pod_spec.security_context.run_as_group == 3000
        assert pod_spec.security_context.supplemental_groups == [4000]


class TestSecurityContextEnvVarDefaults:
    """Tests for environment variable-based security context defaults."""

    def test_env_var_security_context_parsed(self):
        """Verify METAFLOW_KUBERNETES_SECURITY_CONTEXT env var is parsed as JSON."""
        with patch(
            "metaflow.plugins.kubernetes.kubernetes_decorator.KUBERNETES_SECURITY_CONTEXT",
            '{"run_as_user": 1000}',
        ), patch(
            "metaflow.plugins.kubernetes.kubernetes_decorator.KUBERNETES_POD_SECURITY_CONTEXT",
            "",
        ):
            from metaflow.plugins.kubernetes.kubernetes_decorator import (
                KubernetesDecorator,
            )

            deco = KubernetesDecorator.__new__(KubernetesDecorator)
            deco.attributes = dict(KubernetesDecorator.defaults)
            deco.init()
            assert deco.attributes["security_context"] == {"run_as_user": 1000}

    def test_env_var_pod_security_context_parsed(self):
        """Verify METAFLOW_KUBERNETES_POD_SECURITY_CONTEXT env var is parsed as JSON."""
        with patch(
            "metaflow.plugins.kubernetes.kubernetes_decorator.KUBERNETES_SECURITY_CONTEXT",
            "",
        ), patch(
            "metaflow.plugins.kubernetes.kubernetes_decorator.KUBERNETES_POD_SECURITY_CONTEXT",
            '{"fs_group": 2000}',
        ):
            from metaflow.plugins.kubernetes.kubernetes_decorator import (
                KubernetesDecorator,
            )

            deco = KubernetesDecorator.__new__(KubernetesDecorator)
            deco.attributes = dict(KubernetesDecorator.defaults)
            deco.init()
            assert deco.attributes["pod_security_context"] == {"fs_group": 2000}

    def test_decorator_overrides_env_var(self):
        """Verify decorator value takes precedence over env var."""
        with patch(
            "metaflow.plugins.kubernetes.kubernetes_decorator.KUBERNETES_SECURITY_CONTEXT",
            '{"run_as_user": 1000}',
        ), patch(
            "metaflow.plugins.kubernetes.kubernetes_decorator.KUBERNETES_POD_SECURITY_CONTEXT",
            '{"fs_group": 2000}',
        ):
            from metaflow.plugins.kubernetes.kubernetes_decorator import (
                KubernetesDecorator,
            )

            deco = KubernetesDecorator.__new__(KubernetesDecorator)
            deco.attributes = dict(KubernetesDecorator.defaults)
            # Simulate decorator explicitly setting the values
            deco.attributes["security_context"] = {"run_as_user": 5000}
            deco.attributes["pod_security_context"] = {"fs_group": 6000}
            deco.init()
            # Decorator values should be preserved
            assert deco.attributes["security_context"] == {"run_as_user": 5000}
            assert deco.attributes["pod_security_context"] == {"fs_group": 6000}
