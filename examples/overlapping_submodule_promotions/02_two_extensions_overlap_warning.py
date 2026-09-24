#!/usr/bin/env python3
"""
Example 2 – Two Extensions Promote the Same Alias (Overlap Warning)
====================================================================

Two different extension packages both promote ``datatools`` under
``metaflow.plugins``.  The extension system detects the conflict,
emits a ``UserWarning``, and lets the **last-loaded** package win.

What you should see when you run this script:
  ⚠️  One warning identifying the conflicting packages.
  ✅ The second package's target is the resolved alias.
"""

import sys
import os
import types
import warnings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metaflow.extension_support import (
    EXT_PKG,
    alias_submodules,
    _promoted_aliases,
)


def _make_module(org_name):
    """Create a fake extension config module for *org_name*."""
    mod = types.ModuleType("%s.%s.plugins" % (EXT_PKG, org_name))
    mod.__package__ = mod.__name__
    mod.__mf_promote_submodules__ = ["datatools"]
    return mod


def main():
    _promoted_aliases.clear()

    mod_alpha = _make_module("alpha_corp")
    mod_beta = _make_module("beta_corp")

    all_warnings = []

    # --- First extension loads (alpha_corp) ---
    with warnings.catch_warnings(record=True) as w1:
        warnings.simplefilter("always")
        alias_submodules(mod_alpha, "alpha_corp", "plugins")
    all_warnings.extend(w1)

    # --- Second extension loads (beta_corp) – overlap! ---
    with warnings.catch_warnings(record=True) as w2:
        warnings.simplefilter("always")
        aliases = alias_submodules(mod_beta, "beta_corp", "plugins")
    all_warnings.extend(w2)

    print("=== Example 2: Two Extensions Overlap ===\n")
    print("Final alias target:", aliases.get("metaflow.plugins.datatools"))
    print("Warnings emitted:", len(all_warnings))

    for w in all_warnings:
        print("\n⚠️  WARNING:", w.message)

    winner = _promoted_aliases.get("metaflow.plugins.datatools")
    if winner:
        print("\n✅ Winner (last-loaded):", winner[0], "→", winner[1])


if __name__ == "__main__":
    main()
