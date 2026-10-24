#!/usr/bin/env python3
"""
Example 3 – Partial Overlap
============================

Two extension packages promote several submodules each, but only **one**
alias overlaps.  The system should warn about the overlapping alias only,
leaving the unique ones untouched.

What you should see when you run this script:
  ⚠️  One warning (for ``common_utils`` only).
  ✅ All other aliases are registered without warnings.
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


def main():
    _promoted_aliases.clear()

    # alpha_corp promotes: common_utils, alpha_special
    mod_alpha = types.ModuleType("%s.alpha_corp.plugins" % EXT_PKG)
    mod_alpha.__package__ = mod_alpha.__name__
    mod_alpha.__mf_promote_submodules__ = ["common_utils", "alpha_special"]

    # beta_corp promotes: common_utils, beta_special
    mod_beta = types.ModuleType("%s.beta_corp.plugins" % EXT_PKG)
    mod_beta.__package__ = mod_beta.__name__
    mod_beta.__mf_promote_submodules__ = ["common_utils", "beta_special"]

    all_warnings = []

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        aliases_a = alias_submodules(mod_alpha, "alpha_corp", "plugins")
    all_warnings.extend(w)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        aliases_b = alias_submodules(mod_beta, "beta_corp", "plugins")
    all_warnings.extend(w)

    print("=== Example 3: Partial Overlap ===\n")
    print("Aliases from alpha_corp:", list(aliases_a.keys()))
    print("Aliases from beta_corp:", list(aliases_b.keys()))
    print("Total warnings:", len(all_warnings))

    for w in all_warnings:
        print("\n⚠️  WARNING:", w.message)

    print("\nFinal promoted aliases:")
    for alias, (pkg, target) in sorted(_promoted_aliases.items()):
        print("  %s → %s (from %s)" % (alias, target, pkg))


if __name__ == "__main__":
    main()
