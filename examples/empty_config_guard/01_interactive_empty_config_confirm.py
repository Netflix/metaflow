#!/usr/bin/env python
"""
Scenario 1 – Interactive empty-configuration confirmation
==========================================================

Run this script directly in a terminal (not piped) to see the interactive
warning and confirmation prompt that fires when ``persist_env`` receives an
empty configuration dictionary.

Expected behaviour
------------------
1. A yellow warning is printed explaining that the config is empty.
2. A confirmation prompt asks whether you really want to save it.
3. If you answer **N** (default), the file is left untouched.
4. If you answer **y**, the empty config is written.

Usage::

    python examples/empty_config_guard/01_interactive_empty_config_confirm.py
"""

import json
import os
import sys
import tempfile

# ---------------------------------------------------------------------------
# Bootstrap – make sure the repo root is on sys.path so we can import metaflow
# directly from source when running outside of an installed environment.
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from unittest.mock import patch
from metaflow.cmd.configure_cmd import persist_env


def main():
    # Create a temporary config directory with an existing configuration.
    tmpdir = tempfile.mkdtemp(prefix="metaflow_example_")
    config_path = os.path.join(tmpdir, "config.json")

    existing = {"METAFLOW_DEFAULT_DATASTORE": "s3", "METAFLOW_SERVICE_URL": "https://example.com"}
    with open(config_path, "w") as f:
        json.dump(existing, f, indent=4)

    print("=" * 60)
    print("Scenario 1: Interactive empty-config confirmation")
    print("=" * 60)
    print(f"\nExisting config at: {config_path}")
    print(f"Contents: {json.dumps(existing, indent=2)}\n")

    # Patch get_config_path so persist_env writes to our temp file.
    with patch(
        "metaflow.cmd.configure_cmd.get_config_path",
        return_value=config_path,
    ):
        result = persist_env({}, profile="")

    # Show the outcome.
    print()
    if result:
        print("✅ Empty configuration was written (user confirmed).")
    else:
        print("🛑 Write was aborted – configuration unchanged.")

    with open(config_path) as f:
        final = json.load(f)
    print(f"\nFinal config contents: {json.dumps(final, indent=2)}")

    # Cleanup
    os.remove(config_path)
    os.rmdir(tmpdir)


if __name__ == "__main__":
    main()
