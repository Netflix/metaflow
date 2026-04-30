"""Orchestrator CLI paths (Argo / SF / Airflow / `metaflow package`) must
always include metaflow in the tarball because their DAG glue runs outside
any @conda/@pypi env. This file asserts the explicit flag plumbing behaves
correctly regardless of what auto-detect would have chosen."""

from metaflow import FlowSpec, step, conda
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.package import MetaflowPackage


class _EveryStepConda(FlowSpec):
    @conda(packages={"metaflow": "2.12.0"})
    @step
    def start(self):
        self.next(self.end)

    @conda(packages={"metaflow": "2.12.0"})
    @step
    def end(self):
        pass


class _BareFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_explicit_include_true_overrides_autodetect():
    # autodetect for this flow would return False (all steps covered); the
    # explicit flag forces inclusion.
    pkg = MetaflowPackage(
        _EveryStepConda(),
        MetaflowEnvironment(_EveryStepConda),
        echo=lambda *a, **k: None,
        include_mf_distribution=True,
    )
    names = {n for _, n in pkg._mfcontent.content_names()}
    assert any(n.endswith("metaflow/__init__.py") for n in names)


def test_explicit_include_false_overrides_autodetect():
    pkg = MetaflowPackage(
        _BareFlow(),
        MetaflowEnvironment(_BareFlow),
        echo=lambda *a, **k: None,
        include_mf_distribution=False,
    )
    names = {n for _, n in pkg._mfcontent.content_names()}
    assert not any(n.endswith("metaflow/__init__.py") for n in names)
