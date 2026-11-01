#!/usr/bin/env python3
"""
Example 4 – Three-Way Overlap
==============================

Three different extension packages all promote the same ``shared`` alias.
The system emits **two** warnings (one each time a new package overrides
the previous winner) and the very last package wins.

What you should see when you run this script:
  ⚠️  Two warnings.
  ✅ The third package is the final winner.
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
    mod = types.ModuleType("%s.%s.plugins" % (EXT_PKG, org_name))
    mod.__package__ = mod.__name__
    mod.__mf_promote_submodules__ = ["shared"]
    return mod


def main():
    _promoted_aliases.clear()

    packages = ["alpha_corp", "beta_corp", "gamma_corp"]
    all_warnings = []

    for org in packages:
        mod = _make_module(org)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            alias_submodules(mod, org, "plugins")
        all_warnings.extend(w)

    print("=== Example 4: Three-Way Overlap ===\n")
    print("Packages loaded (in order):", packages)
    print("Total warnings:", len(all_warnings))

    for i, w in enumerate(all_warnings, 1):
        print("\n⚠️  Warning #%d: %s" % (i, w.message))

    winner = _promoted_aliases.get("metaflow.plugins.shared")
    if winner:
        print("\n✅ Final winner: %s → %s" % (winner[0], winner[1]))


if __name__ == "__main__":
    main()
