#!/usr/bin/env python3
"""
Example 1 – Single Extension, No Conflict
==========================================

This is the baseline scenario: a single extension package promotes a
submodule and everything works without warnings.

What you should see when you run this script:
  ✅ No warnings emitted.
  ✅ The alias is registered correctly.
"""

import sys
import os
import types
import warnings

# Ensure the repo root is on the path so we can import metaflow
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metaflow.extension_support import (
    EXT_PKG,
    alias_submodules,
    _promoted_aliases,
)


def main():
    # Clean slate
    _promoted_aliases.clear()

    # Simulate a single extension: metaflow_extensions.acme_corp.plugins
    # that promotes "datatools" so it becomes available as
    #   metaflow.plugins.datatools → metaflow_extensions.acme_corp.plugins.datatools
    mod = types.ModuleType("%s.acme_corp.plugins" % EXT_PKG)
    mod.__package__ = mod.__name__
    mod.__mf_promote_submodules__ = ["datatools"]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        aliases = alias_submodules(mod, "acme_corp", "plugins")

    print("=== Example 1: Single Extension, No Conflict ===\n")
    print("Aliases created:", list(aliases.keys()))
    print("Warnings emitted:", len(caught))

    if len(caught) == 0:
        print("\n✅ No conflict – everything is clean.")
    else:
        print("\n❌ Unexpected warnings!")
        for w in caught:
            print("  ", w.message)


if __name__ == "__main__":
    main()
