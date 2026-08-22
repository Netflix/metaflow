import copy

from metaflow.user_configs.config_parameters import DelayEvaluator


def _sample_globals():
    def my_func():
        return "hello"

    return {"my_func": my_func}


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
