#!/usr/bin/env python
"""
Scenario 2 – Non-interactive abort when config is empty
=========================================================

When ``persist_env`` is called with an empty dictionary in a non-interactive
context (stdin is not a TTY — e.g. CI pipelines, piped input), the function
automatically **aborts** without writing, to prevent accidental configuration
loss.

Expected behaviour
------------------
1. A yellow warning is printed explaining that the config is empty.
2. A red abort message is printed (no confirmation prompt).
3. The existing configuration file is left completely untouched.
4. ``persist_env`` returns ``False``.

Usage::

    # Simulate a non-interactive environment by piping stdin
    echo "" | python examples/empty_config_guard/02_non_interactive_empty_config_abort.py

    # Or redirect stdin from /dev/null
    python examples/empty_config_guard/02_non_interactive_empty_config_abort.py < /dev/null
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

    existing = {"METAFLOW_DEFAULT_DATASTORE": "s3"}
    with open(config_path, "w") as f:
        json.dump(existing, f, indent=4)

    print("=" * 60)
    print("Scenario 2: Non-interactive abort on empty config")
    print("=" * 60)
    print(f"\nExisting config at: {config_path}")
    print(f"Contents: {json.dumps(existing, indent=2)}\n")

    with patch(
        "metaflow.cmd.configure_cmd.get_config_path",
        return_value=config_path,
    ):
        result = persist_env({}, profile="")

    print()
    if result:
        print("✅ Configuration was written.")
    else:
        print("🛑 Write was aborted – configuration unchanged (expected).")

    with open(config_path) as f:
        final = json.load(f)
    print(f"\nFinal config contents: {json.dumps(final, indent=2)}")

    # Cleanup
    os.remove(config_path)
    os.rmdir(tmpdir)


if __name__ == "__main__":
    main()
