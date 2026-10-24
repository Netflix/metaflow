"""Tests for SFN from_deployment parameter extraction logic."""

import json
import pytest


def _extract_param_info(env_vars):
    """
    Mirrors the parameter extraction logic in
    StepFunctionsDeployedFlow.from_deployment: given the Environment list
    from ContainerOverrides, return param_info dict.
    """
    param_info = {}
    try:
        env_dict = {item.get("Name"): item.get("Value") for item in env_vars}
        raw = env_dict.get("METAFLOW_DEFAULT_PARAMETERS")
        if raw:
            for pname, pvalue in json.loads(raw).items():
                if isinstance(pvalue, bool):
                    ptype = "bool"
                elif isinstance(pvalue, int):
                    ptype = "int"
                elif isinstance(pvalue, float):
                    ptype = "float"
                else:
                    ptype = "str"
                param_info[pname] = {
                    "name": pname,
                    "python_var_name": pname,
                    "type": ptype,
                    "description": "",
                    "is_required": False,
                }
    except (KeyError, json.JSONDecodeError, TypeError):
        pass
    return param_info


def test_valid_param_extraction():
    env = [
        {
            "Name": "METAFLOW_DEFAULT_PARAMETERS",
            "Value": json.dumps(
                {"name": "alice", "count": 5, "rate": 3.14, "verbose": True}
            ),
        }
    ]
    info = _extract_param_info(env)
    assert info["name"]["type"] == "str"
    assert info["count"]["type"] == "int"
    assert info["rate"]["type"] == "float"
    assert info["verbose"]["type"] == "bool"
    assert all(not v["is_required"] for v in info.values())


def test_bool_detected_before_int():
    """bool is a subclass of int in Python; ensure bool wins."""
    env = [{"Name": "METAFLOW_DEFAULT_PARAMETERS", "Value": json.dumps({"flag": True})}]
    assert _extract_param_info(env)["flag"]["type"] == "bool"


def test_malformed_json_returns_empty():
    env = [{"Name": "METAFLOW_DEFAULT_PARAMETERS", "Value": "{bad json!}"}]
    assert _extract_param_info(env) == {}


def test_missing_env_var_returns_empty():
    env = [{"Name": "OTHER_VAR", "Value": "x"}]
    assert _extract_param_info(env) == {}


def test_empty_env_list_returns_empty():
    assert _extract_param_info([]) == {}
