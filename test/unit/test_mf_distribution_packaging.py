import pytest

from metaflow import FlowSpec, step, conda, pypi, conda_base, pypi_base
from metaflow.package import MetaflowPackage


class _BareFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class _EveryStepConda(FlowSpec):
    @conda(packages={"metaflow": "2.12.0"})
    @step
    def start(self):
        self.next(self.end)

    @conda(packages={"metaflow": "2.12.0"})
    @step
    def end(self):
        pass


class _MixedConda(FlowSpec):
    @conda(packages={"metaflow": "2.12.0"})
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class _DisabledConda(FlowSpec):
    @conda(disabled=True)
    @step
    def start(self):
        self.next(self.end)

    @conda(disabled=True)
    @step
    def end(self):
        pass


@conda_base(packages={"metaflow": "2.12.0"})
class _CondaBaseFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


@pypi_base(packages={"metaflow": "2.12.0"})
class _PypiBaseFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_flow_is_none_includes_mf():
    assert MetaflowPackage._should_include_mf_distribution(None) is True


def test_bare_flow_includes_mf():
    assert MetaflowPackage._should_include_mf_distribution(_BareFlow()) is True


def test_every_step_conda_skips_mf():
    assert MetaflowPackage._should_include_mf_distribution(_EveryStepConda()) is False


def test_mixed_steps_include_mf():
    assert MetaflowPackage._should_include_mf_distribution(_MixedConda()) is True


def test_disabled_conda_includes_mf():
    assert MetaflowPackage._should_include_mf_distribution(_DisabledConda()) is True


def test_conda_base_covers_all_steps():
    assert MetaflowPackage._should_include_mf_distribution(_CondaBaseFlow()) is False


def test_pypi_base_covers_all_steps():
    assert MetaflowPackage._should_include_mf_distribution(_PypiBaseFlow()) is False


@pytest.mark.parametrize("env_value", ["1", "true", "yes"])
def test_config_override_forces_exclude(monkeypatch, env_value):
    monkeypatch.setattr(
        "metaflow.metaflow_config.PACKAGE_EXCLUDE_METAFLOW_DISTRIBUTION", env_value
    )
    assert MetaflowPackage._should_include_mf_distribution(_BareFlow()) is False


@pytest.mark.parametrize("env_value", ["0", "false", "no"])
def test_config_override_forces_include(monkeypatch, env_value):
    monkeypatch.setattr(
        "metaflow.metaflow_config.PACKAGE_EXCLUDE_METAFLOW_DISTRIBUTION", env_value
    )
    assert MetaflowPackage._should_include_mf_distribution(_EveryStepConda()) is True


def test_config_override_invalid_raises(monkeypatch):
    monkeypatch.setattr(
        "metaflow.metaflow_config.PACKAGE_EXCLUDE_METAFLOW_DISTRIBUTION", "maybe"
    )
    with pytest.raises(ValueError):
        MetaflowPackage._should_include_mf_distribution(_BareFlow())
