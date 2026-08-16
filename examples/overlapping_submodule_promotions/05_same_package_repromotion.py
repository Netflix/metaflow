#!/usr/bin/env python3
"""
Example 5 – Same Package Re-promotion (No Warning)
====================================================

If the *same* package promotes the same alias more than once (e.g. because
the module is loaded twice), no warning should be raised – there is no
actual conflict.

What you should see when you run this script:
  ✅ Zero warnings.
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

    mod = types.ModuleType("%s.acme_corp.plugins" % EXT_PKG)
    mod.__package__ = mod.__name__
    mod.__mf_promote_submodules__ = ["datatools"]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # Promote twice from the same package
        alias_submodules(mod, "acme_corp", "plugins")
        alias_submodules(mod, "acme_corp", "plugins")

    print("=== Example 5: Same Package Re-promotion ===\n")
    print("Warnings emitted:", len(caught))

    if len(caught) == 0:
        print("✅ No conflict – same package re-promoting is harmless.")
    else:
        print("❌ Unexpected warnings!")
        for w in caught:
            print("  ", w.message)


if __name__ == "__main__":
    main()
