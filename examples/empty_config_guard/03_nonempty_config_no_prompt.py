#!/usr/bin/env python
"""
Scenario 3 – Non-empty config is written without extra prompts
================================================================

When ``persist_env`` receives a **non-empty** configuration dictionary it
writes it directly — no warning, no confirmation prompt. This is the
existing (unchanged) happy-path behaviour.

Expected behaviour
------------------
1. The config is written immediately.
2. A success message is printed.
3. ``persist_env`` returns ``True``.

Usage::

    python examples/empty_config_guard/03_nonempty_config_no_prompt.py
"""

import json
import os
import sys
import tempfile

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from unittest.mock import patch
from metaflow.cmd.configure_cmd import persist_env


def main():
    tmpdir = tempfile.mkdtemp(prefix="metaflow_example_")
    config_path = os.path.join(tmpdir, "config.json")

    print("=" * 60)
    print("Scenario 3: Non-empty config – direct write, no prompt")
    print("=" * 60)

    new_config = {
        "METAFLOW_DEFAULT_DATASTORE": "s3",
        "METAFLOW_DATASTORE_SYSROOT_S3": "s3://my-bucket/metaflow",
    }
    print(f"\nConfig to write: {json.dumps(new_config, indent=2)}")

    with patch(
        "metaflow.cmd.configure_cmd.get_config_path",
        return_value=config_path,
    ):
        result = persist_env(new_config, profile="")

    print()
    if result:
        print("✅ Configuration written successfully (no prompt needed).")
    else:
        print("❌ Unexpected: write failed.")

    with open(config_path) as f:
        final = json.load(f)
    print(f"\nFinal config contents: {json.dumps(final, indent=2)}")

    # Cleanup
    os.remove(config_path)
    os.rmdir(tmpdir)


if __name__ == "__main__":
    main()
