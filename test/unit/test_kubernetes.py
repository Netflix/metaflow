from unittest.mock import MagicMock

import pytest

from metaflow.plugins.kubernetes.kube_utils import (
    KubernetesException as KubeUtilsException,
    validate_kube_labels,
    parse_kube_keyvalue_list,
)
from metaflow.plugins.kubernetes.kubernetes import KubernetesException
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator


@pytest.mark.parametrize(
    "labels",
    [
        None,
        {"label": "value"},
        {"label1": "val1", "label2": "val2"},
        {"label1": "val1", "label2": None},
        {"label": "a"},
        {"label": ""},
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "123"
            )
        },
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234-_.890"
                "1234567890"
                "1234567890"
                "1234567890"
                "123"
            )
        },
    ],
)
def test_kubernetes_decorator_validate_kube_labels(labels):
    assert validate_kube_labels(labels)


@pytest.mark.parametrize(
    "labels",
    [
        {"label": "a-"},
        {"label": ".a"},
        {"label": "test()"},
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234"
            )
        },
        {"label": "(){}??"},
        {"valid": "test", "invalid": "bißchen"},
    ],
)
def test_kubernetes_decorator_validate_kube_labels_fail(labels):
    """Fail if label contains invalid characters or is too long"""
    with pytest.raises(KubeUtilsException):
        validate_kube_labels(labels)


@pytest.mark.parametrize(
    "items,requires_both,expected",
    [
        (["key=value"], True, {"key": "value"}),
        (["key=value"], False, {"key": "value"}),
        (["key"], False, {"key": None}),
        (["key=value", "key2=value2"], True, {"key": "value", "key2": "value2"}),
    ],
)
def test_kubernetes_parse_keyvalue_list(items, requires_both, expected):
    ret = parse_kube_keyvalue_list(items, requires_both)
    assert ret == expected


@pytest.mark.parametrize(
    "items,requires_both",
    [
        (["key=value", "key=value2"], True),
        (["key"], True),
    ],
)
def test_kubernetes_parse_keyvalue_list(items, requires_both):
    with pytest.raises(KubeUtilsException):
        parse_kube_keyvalue_list(items, requires_both)


class TestKubernetesDecoratorLocalMode:
    """Tests for issue #2588: @kubernetes should be ignored for local runs."""

    def _make_decorator(self, statically_defined=True, **attrs):
        deco = KubernetesDecorator(
            attributes=attrs or None, statically_defined=statically_defined
        )
        return deco

    def test_static_decorator_enters_local_mode_with_local_datastore(self):
        """When @kubernetes is in source code and datastore is local, enter local mode."""
        deco = self._make_decorator(statically_defined=True)
        flow_datastore = MagicMock()
        flow_datastore.TYPE = "local"
        # step_init should not raise; it should silently enter local mode
        deco.step_init(
            flow=MagicMock(),
            graph=MagicMock(),
            step="my_step",
            decos=[],
            environment=MagicMock(),
            flow_datastore=flow_datastore,
            logger=MagicMock(),
        )
        assert deco._local_mode is True

    def test_dynamic_decorator_raises_with_local_datastore(self):
        """When @kubernetes is added via --with and datastore is local, raise error."""
        deco = self._make_decorator(statically_defined=False)
        flow_datastore = MagicMock()
        flow_datastore.TYPE = "local"
        with pytest.raises(KubernetesException, match="--datastore=s3"):
            deco.step_init(
                flow=MagicMock(),
                graph=MagicMock(),
                step="my_step",
                decos=[],
                environment=MagicMock(),
                flow_datastore=flow_datastore,
                logger=MagicMock(),
            )

    def test_local_mode_skips_runtime_step_cli(self):
        """In local mode, runtime_step_cli should not redirect to kubernetes."""
        deco = self._make_decorator(statically_defined=True)
        deco._local_mode = True
        cli_args = MagicMock()
        cli_args.commands = ["step"]
        deco.runtime_step_cli(
            cli_args, retry_count=0, max_user_code_retries=3, ubf_context=None
        )
        # commands should NOT have been changed to ["kubernetes", "step"]
        assert cli_args.commands == ["step"]

    def test_local_mode_skips_package_init(self):
        """In local mode, package_init should be a no-op (no kubernetes import needed)."""
        deco = self._make_decorator(statically_defined=True)
        deco._local_mode = True
        # Should not raise even if kubernetes package is not installed
        deco.package_init(
            flow=MagicMock(), step_name="my_step", environment=MagicMock()
        )

    def test_local_mode_skips_runtime_init(self):
        """In local mode, runtime_init should be a no-op."""
        deco = self._make_decorator(statically_defined=True)
        deco._local_mode = True
        deco.runtime_init(
            flow=MagicMock(), graph=MagicMock(), package=MagicMock(), run_id="123"
        )
        # Should not set self.flow etc.
        assert not hasattr(deco, "flow")

    def test_local_mode_skips_runtime_task_created(self):
        """In local mode, runtime_task_created should be a no-op."""
        deco = self._make_decorator(statically_defined=True)
        deco._local_mode = True
        # Should not raise even though flow_datastore and package are not set
        deco.runtime_task_created(
            task_datastore=MagicMock(),
            task_id="1",
            split_index=None,
            input_paths=[],
            is_cloned=False,
            ubf_context=None,
        )

    def test_s3_datastore_does_not_enter_local_mode(self):
        """With S3 datastore, even a static decorator should NOT enter local mode."""
        deco = self._make_decorator(statically_defined=True)
        flow_datastore = MagicMock()
        flow_datastore.TYPE = "s3"
        # This will proceed with normal K8s setup - it will fail at QoS
        # validation or other checks, but should NOT set _local_mode
        try:
            deco.step_init(
                flow=MagicMock(),
                graph=MagicMock(),
                step="my_step",
                decos=[],
                environment=MagicMock(),
                flow_datastore=flow_datastore,
                logger=MagicMock(),
            )
        except Exception:
            pass  # Expected to fail on later validation
        assert deco._local_mode is False


class TestKubernetesDecoratorPriorityClass:
    """Tests for issue #1752: priority_class option for @kubernetes."""

    def test_priority_class_default_is_from_config(self):
        """priority_class should default to KUBERNETES_PRIORITY_CLASS config value."""
        from metaflow.metaflow_config import KUBERNETES_PRIORITY_CLASS

        deco = KubernetesDecorator()
        assert deco.attributes["priority_class"] == KUBERNETES_PRIORITY_CLASS

    def test_priority_class_can_be_set(self):
        """priority_class should be settable via decorator attributes."""
        deco = KubernetesDecorator(attributes={"priority_class": "high-priority"})
        assert deco.attributes["priority_class"] == "high-priority"

    def test_priority_class_in_defaults(self):
        """priority_class should be in the decorator defaults."""
        assert "priority_class" in KubernetesDecorator.defaults
