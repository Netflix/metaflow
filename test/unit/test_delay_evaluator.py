import copy

from metaflow.flowspec import FlowStateItems
from metaflow.parameters import current_flow
from metaflow.user_configs.config_parameters import DelayEvaluator


def _sample_globals():
    def my_func():
        return "hello"

    return {"my_func": my_func}


class _DummyFlow:
    _flow_state = {FlowStateItems.CONFIGS: {}}


def _with_dummy_flow(fn):
    current_flow.flow_cls = _DummyFlow
    try:
        return fn()
    finally:
        del current_flow.flow_cls


def test_copy_preserves_saved_globals():
    saved = _sample_globals()
    evaluator = DelayEvaluator("config", saved_globals=saved)
    copied = copy.copy(evaluator)
    assert copied._globals is saved
    assert copied._globals["my_func"]() == "hello"


def test_deepcopy_preserves_saved_globals():
    saved = _sample_globals()
    evaluator = DelayEvaluator("config", saved_globals=saved)
    copied = copy.deepcopy(evaluator)
    assert copied._globals is saved
    assert copied._globals["my_func"]() == "hello"


def test_getattr_preserves_saved_globals():
    saved = _sample_globals()
    evaluator = DelayEvaluator("config", saved_globals=saved)
    chained = evaluator.project
    assert chained._globals is saved
    assert chained._access == ["project"]


def test_getitem_preserves_saved_globals():
    saved = _sample_globals()
    evaluator = DelayEvaluator("config", saved_globals=saved)
    chained = evaluator["project"]
    assert chained._globals is saved
    assert chained._access == ["project"]


def test_chained_call_uses_saved_globals():
    class _Cfg:
        project = "from-globals"

    saved = {"my_func": _Cfg}
    evaluator = DelayEvaluator("my_func", saved_globals=saved)
    chained = evaluator.project
    assert _with_dummy_flow(chained) == "from-globals"


def test_copied_call_uses_saved_globals():
    saved = _sample_globals()
    evaluator = DelayEvaluator("my_func()", saved_globals=saved)
    copied = copy.copy(evaluator)
    assert _with_dummy_flow(copied) == "hello"
