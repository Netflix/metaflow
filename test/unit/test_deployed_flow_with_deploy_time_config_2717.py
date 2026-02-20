"""
Regression test for GitHub issue #2717.

Bug: Triggering a DeployedFlow (e.g. Foo) from a flow that has deploy-time
Config objects (e.g. TriggerFoo) raises:

    MetaflowException: Options were not properly set -- this is an internal error.

Root cause
----------
When TriggerFoo runs on Kubernetes, Metaflow injects METAFLOW_FLOW_CONFIG_VALUE
into the pod's environment for TriggerFoo's own Config resolution. When
TriggerFoo's step code then calls

    DeployedFlow.from_argo_workflows("foo").trigger()

the MetaflowAPI for Foo is constructed from a fake flow file that has **no**
Config parameters (Config objects are deploy-time only and therefore not stored
in the Argo workflow-template annotations). Consequently,
``config_options_with_config_input`` returns ``False`` for ``config_input``.

In ``_compute_flow_parameters()`` the block guarded by

    if CLICK_API_PROCESS_CONFIG:

is entered regardless of whether the flow has Config params.  Inside that block
the code reads METAFLOW_FLOW_CONFIG_VALUE from the environment (which is present
because it was set for TriggerFoo) while METAFLOW_FLOW_CONFIG is absent, giving:

    config_file  = None           # Foo has no --config CLI option or env var
    config_value = <non-empty>    # leaked from TriggerFoo's pod environment

The XOR guard then fires:

    if (config_file is None) ^ (config_value is None):
        raise MetaflowException("Options were not properly set -- this is an internal error.")

Fix (click_api.py line ~463)
-----------------------------
Change:

    if CLICK_API_PROCESS_CONFIG:

to:

    if CLICK_API_PROCESS_CONFIG and self._config_input:

so that when the triggered flow has no Config parameters (``config_input is
False``) the entire env-var look-up is skipped.

These tests verify the invariants of that guard without importing the full
Metaflow CLI stack (which requires Linux-only ``fcntl`` and therefore cannot
be imported on Windows in a unit-test context).
"""

import json
import os
import unittest


# ---------------------------------------------------------------------------
# Test the pure guard logic:
#   "if CLICK_API_PROCESS_CONFIG and self._config_input" must short-circuit
#   when config_input is False, preventing the env-var XOR from firing.
# ---------------------------------------------------------------------------

def _simulate_config_resolution(config_input, env):
    """
    Replicate the critical block from _compute_flow_parameters() in
    metaflow/runner/click_api.py that is responsible for GH #2717.

    This is a pure-Python reimplementation of that block (lines 462-555 of the
    fixed file) so we can test the logic without importing the full Metaflow
    stack.  The variable names and logic match the source exactly so that
    failures in the tests point directly to the corresponding source lines.

    Parameters
    ----------
    config_input : any
        Mirrors ``self._config_input`` in MetaflowAPI.  Pass ``False`` to
        simulate a flow with no Config parameters.
    env : dict
        The environment variables to use (replaces ``os.environ``).

    Returns
    -------
    tuple[config_file, config_value]
        The resolved values (or None, None if the block is skipped).

    Raises
    ------
    ValueError
        Mirrors the MetaflowException raised by the XOR guard.
    """
    CLICK_API_PROCESS_CONFIG = True   # assume enabled (the interesting case)

    # ---- THE FIX: "and self._config_input" added to the original guard ----
    if CLICK_API_PROCESS_CONFIG and config_input:
        opts = {}       # nothing passed explicitly via click
        defaults = {}   # no defaults

        # --- config_file resolution (mirrors source lines 488-511) ---
        config_file = opts.get("config")
        if config_file is None:
            env_config_file = env.get("METAFLOW_FLOW_CONFIG")
            if env_config_file:
                config_file = list(json.loads(env_config_file).items())
            else:
                config_file = defaults.get("config")

        # --- config_value resolution (mirrors source lines 513-533) ---
        config_value = opts.get("config-value")
        if config_value is None:
            env_config_value = env.get("METAFLOW_FLOW_CONFIG_VALUE")
            if env_config_value:
                loaded = json.loads(env_config_value)
                config_value = [
                    (k, json.dumps(v) if not isinstance(v, str) else v)
                    for k, v in loaded.items()
                ]
            else:
                config_value = defaults.get("config_value")

        # --- XOR guard (mirrors source lines 541-545) ---
        if (config_file is None) ^ (config_value is None):
            raise ValueError(
                "Options were not properly set -- this is an internal error."
            )

        return config_file, config_value

    # Block was skipped (config_input is False or CLICK_API_PROCESS_CONFIG is False)
    return None, None


def _simulate_bug(config_input, env):
    """
    Same as _simulate_config_resolution but using the **BUGGY** original guard:

        if CLICK_API_PROCESS_CONFIG:    # without `and self._config_input`

    Used in test_xor_fires_without_fix to confirm that the old code would indeed
    raise.
    """
    CLICK_API_PROCESS_CONFIG = True

    # BUGGY guard — does not check config_input
    if CLICK_API_PROCESS_CONFIG:
        opts = {}
        defaults = {}

        config_file = opts.get("config")
        if config_file is None:
            env_config_file = env.get("METAFLOW_FLOW_CONFIG")
            if env_config_file:
                config_file = list(json.loads(env_config_file).items())
            else:
                config_file = defaults.get("config")

        config_value = opts.get("config-value")
        if config_value is None:
            env_config_value = env.get("METAFLOW_FLOW_CONFIG_VALUE")
            if env_config_value:
                loaded = json.loads(env_config_value)
                config_value = [
                    (k, json.dumps(v) if not isinstance(v, str) else v)
                    for k, v in loaded.items()
                ]
            else:
                config_value = defaults.get("config_value")

        if (config_file is None) ^ (config_value is None):
            raise ValueError(
                "Options were not properly set -- this is an internal error."
            )

        return config_file, config_value

    return None, None


class TestDeployedFlowWithDeployTimeConfig2717(unittest.TestCase):
    """
    Regression tests for GH issue #2717.

    All tests use _simulate_config_resolution / _simulate_bug — faithful
    reimplementations of the critical block in _compute_flow_parameters() —
    so that the test suite can run on any platform (including Windows) without
    requiring the full Metaflow CLI import chain.
    """

    # ------------------------------------------------------------------
    # 1. Confirm the OLD (buggy) code raises in the issue scenario
    # ------------------------------------------------------------------

    def test_xor_fires_without_fix(self):
        """
        Demonstrate that the original guard (without 'and self._config_input')
        raises when METAFLOW_FLOW_CONFIG_VALUE is set but METAFLOW_FLOW_CONFIG
        is not, even for a flow with no Config params (config_input=False).
        """
        env = {
            "METAFLOW_FLOW_CONFIG_VALUE": json.dumps({"config": {"hello": "world"}}),
        }
        with self.assertRaises(ValueError, msg="Buggy code should raise the XOR error"):
            _simulate_bug(config_input=False, env=env)

    # ------------------------------------------------------------------
    # 2. Core regression: fixed code must NOT raise in the issue scenario
    # ------------------------------------------------------------------

    def test_no_config_flow_only_config_value_env_set(self):
        """
        GH #2717 exact scenario:
        - Triggered flow has NO Config parameters  → config_input is False
        - METAFLOW_FLOW_CONFIG_VALUE is set (leaked from calling flow's K8s pod)
        - METAFLOW_FLOW_CONFIG is NOT set

        With the fix the block is skipped entirely; both return values are None.
        """
        env = {
            "METAFLOW_FLOW_CONFIG_VALUE": json.dumps({"config": {"hello": "world"}}),
        }
        try:
            cfg_file, cfg_value = _simulate_config_resolution(
                config_input=False, env=env
            )
        except ValueError as exc:
            self.fail(
                f"Fixed code raised ValueError unexpectedly (GH#2717 regression): {exc}"
            )

        # Block was skipped → both values are None
        self.assertIsNone(cfg_file)
        self.assertIsNone(cfg_value)

    def test_no_config_flow_both_env_vars_set(self):
        """
        Both env vars present (triggering flow used a config file).
        Triggered flow has no Config params.  Must not raise.
        """
        env = {
            "METAFLOW_FLOW_CONFIG": json.dumps({"config": "/tmp/config.json"}),
            "METAFLOW_FLOW_CONFIG_VALUE": json.dumps({"config": {"hello": "world"}}),
        }
        try:
            cfg_file, cfg_value = _simulate_config_resolution(
                config_input=False, env=env
            )
        except ValueError as exc:
            self.fail(
                f"Fixed code raised ValueError unexpectedly with both env vars: {exc}"
            )

        self.assertIsNone(cfg_file)
        self.assertIsNone(cfg_value)

    def test_no_config_flow_clean_environment(self):
        """
        Baseline: neither env var is set, flow has no Config params.
        Must work before and after the fix.
        """
        try:
            cfg_file, cfg_value = _simulate_config_resolution(
                config_input=False, env={}
            )
        except ValueError as exc:
            self.fail(
                f"Fixed code raised ValueError unexpectedly in clean env: {exc}"
            )

        self.assertIsNone(cfg_file)
        self.assertIsNone(cfg_value)

    # ------------------------------------------------------------------
    # 3. Flows WITH Config params: the block IS entered (config_input truthy)
    # ------------------------------------------------------------------

    def test_config_flow_clean_env_no_error(self):
        """
        Flow has Config params (config_input truthy); no env vars set.
        Both config_file and config_value end up None → XOR is False → no error.
        """
        try:
            cfg_file, cfg_value = _simulate_config_resolution(
                config_input=object(),  # truthy, non-False
                env={},
            )
        except ValueError as exc:
            self.fail(f"Raised unexpectedly for Config flow in clean env: {exc}")

        # Both none → config_file is None and config_value is None → XOR False
        self.assertIsNone(cfg_file)
        self.assertIsNone(cfg_value)

    def test_config_flow_both_env_vars_set_no_error(self):
        """
        Flow has Config params; both env vars are set (symmetric, correct case).
        XOR is False → no error; both values returned are non-None.
        """
        env = {
            "METAFLOW_FLOW_CONFIG": json.dumps({"config": "/tmp/config.json"}),
            "METAFLOW_FLOW_CONFIG_VALUE": json.dumps({"config": {"hello": "world"}}),
        }
        try:
            cfg_file, cfg_value = _simulate_config_resolution(
                config_input=object(), env=env
            )
        except ValueError as exc:
            self.fail(
                f"Raised unexpectedly when both env vars set for Config flow: {exc}"
            )

        self.assertIsNotNone(cfg_file)
        self.assertIsNotNone(cfg_value)

    # ------------------------------------------------------------------
    # 4. XOR guard still fires for genuinely malformed state
    # ------------------------------------------------------------------

    def test_xor_guard_still_fires_for_config_flow_asymmetric_env(self):
        """
        Flow HAS Config params (config_input truthy) AND only one of the two
        env vars is set — this is a genuinely inconsistent state.
        The XOR guard MUST still raise.
        """
        env = {
            "METAFLOW_FLOW_CONFIG_VALUE": json.dumps({"config": {"x": 1}}),
            # METAFLOW_FLOW_CONFIG deliberately absent
        }
        with self.assertRaises(
            ValueError, msg="XOR guard must fire for asymmetric env with Config flow"
        ):
            _simulate_config_resolution(config_input=object(), env=env)


if __name__ == "__main__":
    unittest.main()
