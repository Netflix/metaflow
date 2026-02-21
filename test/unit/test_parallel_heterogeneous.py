"""
Unit tests for heterogeneous @parallel cluster support (issue #2501).

Covers:
- ParallelDecorator validation of worker_resources / control_resources
- JobSetSpec new setter methods (disk, gpu, node_selector, tmpfs_size)
- BatchJob._build_resource_requirements static helper
- BatchDecorator warning for unsupported resource keys
"""
import pytest
import warnings
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# ParallelDecorator validation
# ---------------------------------------------------------------------------


def _make_parallel_decorator(worker_resources=None, control_resources=None):
    """Helper: construct a ParallelDecorator with given attributes."""
    from metaflow.plugins.parallel_decorator import ParallelDecorator

    attrs = {}
    if worker_resources is not None:
        attrs["worker_resources"] = worker_resources
    if control_resources is not None:
        attrs["control_resources"] = control_resources
    deco = ParallelDecorator(attributes=attrs, statically_defined=True)
    return deco


def _run_step_init(deco):
    """Call step_init on a decorator with a minimal mock graph/flow."""
    from metaflow.exception import MetaflowException

    # Build a minimal mock that satisfies step_init
    flow = MagicMock()
    graph = MagicMock()
    graph.__getitem__ = MagicMock(return_value=MagicMock(type="linear"))
    environment = MagicMock()
    flow_datastore = MagicMock()
    logger = MagicMock()

    deco.step_init(
        flow,
        graph,
        "my_step",
        [],
        environment,
        flow_datastore,
        logger,
    )


class TestParallelDecoratorDefaults:
    def test_defaults_include_worker_and_control_resources(self):
        from metaflow.plugins.parallel_decorator import ParallelDecorator

        assert "worker_resources" in ParallelDecorator.defaults
        assert "control_resources" in ParallelDecorator.defaults
        assert ParallelDecorator.defaults["worker_resources"] is None
        assert ParallelDecorator.defaults["control_resources"] is None


class TestParallelDecoratorValidation:
    @pytest.mark.parametrize(
        "worker_resources",
        [
            None,
            {"cpu": 8},
            {"cpu": 8, "memory": 32768},
            {"cpu": 2, "gpu": 4, "memory": 16384},
            {"disk": 20480},
            {"tmpfs_size": 1024},
            {"node_selector": {"kubernetes.io/os": "linux"}},
            {"cpu": 0},  # 0 is valid (non-negative)
        ],
    )
    def test_valid_worker_resources(self, worker_resources):
        deco = _make_parallel_decorator(worker_resources=worker_resources)
        _run_step_init(deco)  # should not raise

    @pytest.mark.parametrize(
        "control_resources",
        [
            None,
            {"cpu": 2, "memory": 4096},
            {"gpu": 0},
        ],
    )
    def test_valid_control_resources(self, control_resources):
        deco = _make_parallel_decorator(control_resources=control_resources)
        _run_step_init(deco)  # should not raise

    @pytest.mark.parametrize(
        "worker_resources",
        [
            "not_a_dict",
            42,
            ["cpu", 8],
        ],
    )
    def test_worker_resources_must_be_dict(self, worker_resources):
        from metaflow.exception import MetaflowException

        deco = _make_parallel_decorator(worker_resources=worker_resources)
        with pytest.raises(MetaflowException, match="must be a dictionary"):
            _run_step_init(deco)

    @pytest.mark.parametrize(
        "resources",
        [
            {"bad_key": 1},
            {"cpu": 2, "invalid_key": "value"},
            {"unknown": 100},
        ],
    )
    def test_invalid_resource_keys_raise(self, resources):
        from metaflow.exception import MetaflowException

        deco = _make_parallel_decorator(worker_resources=resources)
        with pytest.raises(MetaflowException, match="invalid keys"):
            _run_step_init(deco)

    @pytest.mark.parametrize(
        "resources",
        [
            {"cpu": -1},
            {"memory": -512},
            {"gpu": -2},
            {"disk": -100},
            {"tmpfs_size": -1},
            {"cpu": "not_a_number"},
        ],
    )
    def test_negative_or_invalid_numeric_values_raise(self, resources):
        from metaflow.exception import MetaflowException

        deco = _make_parallel_decorator(worker_resources=resources)
        with pytest.raises(MetaflowException, match="non-negative number"):
            _run_step_init(deco)

    def test_control_resources_invalid_key_raises(self):
        from metaflow.exception import MetaflowException

        deco = _make_parallel_decorator(control_resources={"bad_key": 1})
        with pytest.raises(MetaflowException, match="invalid keys"):
            _run_step_init(deco)

    def test_both_valid_simultaneously(self):
        deco = _make_parallel_decorator(
            worker_resources={"cpu": 8, "gpu": 4, "memory": 32768},
            control_resources={"cpu": 2, "memory": 4096},
        )
        _run_step_init(deco)  # should not raise


# ---------------------------------------------------------------------------
# JobSetSpec new setter methods
# ---------------------------------------------------------------------------


def _make_jobset_spec():
    """Return a minimal JobSetSpec instance without actually connecting to K8s."""
    from metaflow.plugins.kubernetes.kubernetes_jobsets import JobSetSpec

    # JobSetSpec.__init__ just stores kwargs; pass minimum required keys
    minimal_kwargs = {
        "name": "test-spec",
        "namespace": "default",
        "service_account": None,
        "node_selector": None,
        "image": "python:3.9",
        "image_pull_policy": None,
        "image_pull_secrets": None,
        "cpu": "1",
        "memory": "4096",
        "disk": "10240",
        "gpu": None,
        "gpu_vendor": "nvidia",
        "timeout_in_seconds": 3600,
        "retries": 0,
        "use_tmpfs": None,
        "tmpfs_tempdir": True,
        "tmpfs_size": None,
        "tmpfs_path": "/metaflow_temp",
        "persistent_volume_claims": None,
        "tolerations": None,
        "shared_memory": None,
        "port": None,
        "qos": None,
        "labels": None,
        "annotations": None,
        "security_context": None,
        "environment_variables": {},
        "environment_variables_from_selectors": {},
        "secrets": [],
    }
    spec = JobSetSpec.__new__(JobSetSpec)
    spec._kwargs = dict(minimal_kwargs)
    spec._kubernetes_sdk = MagicMock()
    spec.name = minimal_kwargs["name"]
    return spec


class TestJobSetSpecNewSetters:
    def test_disk_setter_updates_kwargs(self):
        spec = _make_jobset_spec()
        result = spec.disk(20480)
        assert spec._kwargs["disk"] == 20480
        assert result is spec  # returns self for chaining

    def test_gpu_setter_updates_kwargs(self):
        spec = _make_jobset_spec()
        result = spec.gpu(4)
        assert spec._kwargs["gpu"] == 4
        assert result is spec

    def test_node_selector_setter_updates_kwargs(self):
        spec = _make_jobset_spec()
        ns = {"kubernetes.io/os": "linux", "accelerator": "gpu"}
        result = spec.node_selector(ns)
        assert spec._kwargs["node_selector"] == ns
        assert result is spec

    def test_tmpfs_size_setter_updates_kwargs(self):
        spec = _make_jobset_spec()
        result = spec.tmpfs_size(1024)
        assert spec._kwargs["tmpfs_size"] == 1024
        assert result is spec

    def test_setters_are_chainable(self):
        spec = _make_jobset_spec()
        # All new setters plus existing ones should chain cleanly
        result = spec.cpu("2").memory("8192").disk(20480).gpu(4).tmpfs_size(512)
        assert result is spec
        assert spec._kwargs["cpu"] == "2"
        assert spec._kwargs["memory"] == "8192"
        assert spec._kwargs["disk"] == 20480
        assert spec._kwargs["gpu"] == 4
        assert spec._kwargs["tmpfs_size"] == 512

    def test_gpu_setter_overrides_existing_value(self):
        spec = _make_jobset_spec()
        spec.gpu(2)
        spec.gpu(8)
        assert spec._kwargs["gpu"] == 8


# ---------------------------------------------------------------------------
# BatchJob._build_resource_requirements
# ---------------------------------------------------------------------------


class TestBuildResourceRequirements:
    def _invoke(self, resources_dict, baseline_reqs):
        from metaflow.plugins.aws.batch.batch_client import BatchJob

        return BatchJob._build_resource_requirements(resources_dict, baseline_reqs)

    def _as_dict(self, reqs_list):
        return {r["type"]: r["value"] for r in reqs_list}

    def test_override_cpu_only(self):
        baseline = [{"type": "VCPU", "value": "1"}, {"type": "MEMORY", "value": "4096"}]
        result = self._invoke({"cpu": 8}, baseline)
        d = self._as_dict(result)
        assert d["VCPU"] == "8"
        assert d["MEMORY"] == "4096"  # unchanged

    def test_override_memory_only(self):
        baseline = [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "4096"}]
        result = self._invoke({"memory": 32768}, baseline)
        d = self._as_dict(result)
        assert d["MEMORY"] == "32768"
        assert d["VCPU"] == "2"

    def test_add_gpu(self):
        baseline = [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "4096"}]
        result = self._invoke({"gpu": 4}, baseline)
        d = self._as_dict(result)
        assert d["GPU"] == "4"
        assert d["VCPU"] == "2"

    def test_remove_gpu_when_set_to_zero(self):
        baseline = [
            {"type": "VCPU", "value": "2"},
            {"type": "MEMORY", "value": "4096"},
            {"type": "GPU", "value": "4"},
        ]
        result = self._invoke({"gpu": 0}, baseline)
        d = self._as_dict(result)
        assert "GPU" not in d

    def test_override_all_three(self):
        baseline = [
            {"type": "VCPU", "value": "1"},
            {"type": "MEMORY", "value": "4096"},
        ]
        result = self._invoke({"cpu": 4, "memory": 16384, "gpu": 2}, baseline)
        d = self._as_dict(result)
        assert d["VCPU"] == "4"
        assert d["MEMORY"] == "16384"
        assert d["GPU"] == "2"

    def test_empty_override_returns_baseline_unchanged(self):
        baseline = [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "4096"}]
        result = self._invoke({}, baseline)
        assert self._as_dict(result) == self._as_dict(baseline)

    def test_none_values_in_override_are_ignored(self):
        baseline = [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "4096"}]
        result = self._invoke({"cpu": None, "memory": None, "gpu": None}, baseline)
        assert self._as_dict(result) == self._as_dict(baseline)

    def test_empty_baseline(self):
        result = self._invoke({"cpu": 4, "memory": 8192}, [])
        d = self._as_dict(result)
        assert d["VCPU"] == "4"
        assert d["MEMORY"] == "8192"

    def test_cpu_float_formatted_correctly(self):
        # e.g. cpu=4.0 → "4", cpu=0.25 → "0.25"
        baseline = []
        result = self._invoke({"cpu": 4.0}, baseline)
        d = self._as_dict(result)
        assert d["VCPU"] == "4"

        result2 = self._invoke({"cpu": 0.25}, baseline)
        d2 = self._as_dict(result2)
        assert d2["VCPU"] == "0.25"

    def test_memory_truncated_to_int(self):
        baseline = []
        result = self._invoke({"memory": 4096.7}, baseline)
        d = self._as_dict(result)
        assert d["MEMORY"] == "4096"


# ---------------------------------------------------------------------------
# BatchDecorator warning for unsupported resource keys
# ---------------------------------------------------------------------------


def _make_batch_decorator_and_run_step_init(worker_resources=None, control_resources=None):
    """
    Build a minimal BatchDecorator and run step_init with a @parallel decorator
    carrying the given resource overrides.
    """
    from metaflow.plugins.aws.batch.batch_decorator import BatchDecorator
    from metaflow.plugins.parallel_decorator import ParallelDecorator

    # Build @parallel decorator with resource overrides
    parallel_attrs = {}
    if worker_resources is not None:
        parallel_attrs["worker_resources"] = worker_resources
    if control_resources is not None:
        parallel_attrs["control_resources"] = control_resources
    parallel_deco = ParallelDecorator(attributes=parallel_attrs, statically_defined=True)

    # Build @batch decorator with minimal defaults
    batch_deco = BatchDecorator(attributes={}, statically_defined=True)

    # Build minimal mocks for step_init
    flow = MagicMock()
    graph = MagicMock()
    graph.__getitem__ = MagicMock(return_value=MagicMock(type="linear"))
    environment = MagicMock()
    flow_datastore = MagicMock()
    flow_datastore.TYPE = "s3"  # BatchDecorator requires --datastore=s3
    logger = MagicMock()

    # decos list includes both decorators
    decos = [parallel_deco, batch_deco]

    batch_deco.step_init(
        flow,
        graph,
        "my_step",
        decos,
        environment,
        flow_datastore,
        logger,
    )
    return batch_deco


class TestBatchDecoratorUnsupportedResourceKeyWarning:
    @pytest.mark.parametrize(
        "worker_resources,expected_keys_in_warning",
        [
            ({"node_selector": {"k": "v"}}, ["node_selector"]),
            ({"tmpfs_size": 512}, ["tmpfs_size"]),
            ({"disk": 20480}, ["disk"]),
            (
                {"disk": 20480, "node_selector": {"k": "v"}, "cpu": 8},
                ["disk", "node_selector"],
            ),
        ],
    )
    def test_warns_for_unsupported_batch_keys_in_worker_resources(
        self, worker_resources, expected_keys_in_warning
    ):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _make_batch_decorator_and_run_step_init(worker_resources=worker_resources)

        batch_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(batch_warnings) == 1
        msg = str(batch_warnings[0].message)
        assert "worker_resources" in msg
        for key in expected_keys_in_warning:
            assert key in msg

    def test_warns_for_unsupported_batch_keys_in_control_resources(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _make_batch_decorator_and_run_step_init(
                control_resources={"tmpfs_size": 512, "memory": 4096}
            )

        batch_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(batch_warnings) == 1
        msg = str(batch_warnings[0].message)
        assert "control_resources" in msg
        assert "tmpfs_size" in msg
        # memory is a supported key — it must NOT appear in the ignored-keys list portion
        # (it does appear in the "Supported keys" footer, which is expected)
        assert "memory" not in msg.split("will be ignored:")[1].split(".")[0]

    def test_both_overrides_warn_independently(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _make_batch_decorator_and_run_step_init(
                worker_resources={"disk": 20480},
                control_resources={"node_selector": {"k": "v"}},
            )

        batch_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(batch_warnings) == 2
        messages = [str(w.message) for w in batch_warnings]
        assert any("worker_resources" in m for m in messages)
        assert any("control_resources" in m for m in messages)

    def test_no_warning_for_supported_keys_only(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _make_batch_decorator_and_run_step_init(
                worker_resources={"cpu": 8, "memory": 32768, "gpu": 4}
            )

        batch_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(batch_warnings) == 0

    def test_no_warning_when_no_resources_set(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _make_batch_decorator_and_run_step_init()

        batch_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(batch_warnings) == 0
