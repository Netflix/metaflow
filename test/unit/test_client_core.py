from types import SimpleNamespace

from metaflow.client.core import Metaflow, MetaflowData


def test_metaflow_data_dir_includes_artifacts():
    artifacts = [SimpleNamespace(id="artifact_a"), SimpleNamespace(id="artifact_b")]
    data = MetaflowData(artifacts)

    values = dir(data)
    assert "artifact_a" in values
    assert "artifact_b" in values


def test_metaflow_data_ipython_key_completions_returns_artifacts():
    artifacts = [SimpleNamespace(id="alpha"), SimpleNamespace(id="beta")]
    data = MetaflowData(artifacts)

    assert sorted(data._ipython_key_completions_()) == ["alpha", "beta"]


def test_metaflow_data_ipython_key_completions_safe_fallback():
    data = MetaflowData.__new__(MetaflowData)
    assert data._ipython_key_completions_() == []


def test_metaflow_data_dir_safe_fallback():
    data = MetaflowData.__new__(MetaflowData)
    result = dir(data)
    # Should not raise; artifact names just won't appear
    assert isinstance(result, list)


def test_metaflow_ipython_key_completions_returns_flow_ids():
    class DummyMetaflow(Metaflow):
        def __iter__(self):
            yield SimpleNamespace(id="HelloFlow")
            yield SimpleNamespace(id="MyPipeline")

    mf = DummyMetaflow.__new__(DummyMetaflow)
    assert mf._ipython_key_completions_() == ["HelloFlow", "MyPipeline"]


def test_metaflow_ipython_key_completions_safe_fallback():
    class BrokenMetaflow(Metaflow):
        def __iter__(self):
            raise RuntimeError("metadata unavailable")

    mf = BrokenMetaflow.__new__(BrokenMetaflow)
    assert mf._ipython_key_completions_() == []
