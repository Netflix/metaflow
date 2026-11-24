"""Regression coverage for local card storage root consistency (issue #2139).

Background
----------
Issue #2139 reported that, with a configured local datastore sysroot, flow
artifacts and `@card` HTML ended up in *different* directories: artifacts under
`METAFLOW_DATASTORE_SYSROOT_LOCAL`, but cards under a `.metaflow/mf.cards`
directory created relative to the current working directory. The cards were
therefore "missing" from the location the user mounted into their UI pod.

Why this is an end-to-end subprocess test
------------------------------------------
The bug lives in the seam between the *writer* and the *reader*, not inside a
single function:

  * the writer is a separate ``<flow>.py card create`` subprocess spawned by the
    card decorator during the run, and
  * the reader (``card list`` / the client) resolves the card root from the
    task's persisted ``ds-root`` metadata.

A unit test on ``CardDatastore.get_storage_root`` can prove the resolved path is
correct, but only a real run can prove that a card written by the subprocess is
found again through the public CLI. This test crosses that boundary on purpose.

The unit-level coverage for each resolution branch lives in
``test/unit/test_card_datastore.py``.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_card_round_trip_uses_configured_local_datastore_root(tmp_path):
    """Cards must land under the configured datastore sysroot and be readable.

    Fails before the fix (cards are written under ``<cwd>/.metaflow/mf.cards``),
    passes after it (cards are written under
    ``<sysroot>/.metaflow/mf.cards`` and the ``card list`` reader finds them).
    """
    # `work_dir` is the cwd we run the flow from; it must stay free of a
    # `.metaflow` directory. `shared_root` is the explicitly configured
    # datastore sysroot where BOTH artifacts and cards are expected to land.
    work_dir = tmp_path / "work"
    shared_root = tmp_path / "shared"
    # Isolated METAFLOW_HOME so a developer's ~/.metaflowconfig cannot influence
    # the run (e.g. flip metadata to a live service).
    config_home = tmp_path / "config"
    work_dir.mkdir()
    config_home.mkdir()

    flow_file = work_dir / "card_root_regression_flow.py"
    flow_file.write_text(
        textwrap.dedent(
            """
            from metaflow import FlowSpec, card, step


            class CardRootRegressionFlow(FlowSpec):
                @card(type="blank")
                @step
                def start(self):
                    self.value = "round-trip"
                    self.next(self.end)

                @step
                def end(self):
                    print(self.value)


            if __name__ == "__main__":
                CardRootRegressionFlow()
            """
        ).lstrip()
    )

    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    env.update(
        {
            "METAFLOW_DEFAULT_DATASTORE": "local",
            "METAFLOW_DEFAULT_METADATA": "local",
            "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(shared_root),
            "METAFLOW_HOME": str(config_home),
            "PYTHONPATH": (
                "%s:%s" % (REPOSITORY_ROOT, pythonpath)
                if pythonpath
                else str(REPOSITORY_ROOT)
            ),
        }
    )
    # Keep the test deterministic: no explicit card root (that path is covered
    # by unit tests), and no inherited profile/service settings.
    env.pop("METAFLOW_CARD_LOCALROOT", None)
    env.pop("METAFLOW_PROFILE", None)
    env.pop("METAFLOW_SERVICE_URL", None)

    # Writer side: run the flow. The @card decorator spawns a separate
    # `card create` subprocess, so this exercises the real write path.
    run_result = subprocess.run(
        [sys.executable, str(flow_file), "run"],
        cwd=str(work_dir),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    # `local` datastore stores everything under `<sysroot>/.metaflow`, and cards
    # belong under the `mf.cards` sibling of the flow artifacts.
    datastore_root = shared_root / ".metaflow"
    card_root = datastore_root / "mf.cards"
    card_files = list(card_root.rglob("*.html"))

    # Core assertion for #2139: the card was written under the configured root...
    assert card_files, (
        "Expected cards below configured datastore root %s.\n"
        "Flow output:\n%s%s"
        % (card_root, run_result.stdout, run_result.stderr)
    )
    # ...and NOT in a stray `.metaflow` created next to the flow script.
    assert not (work_dir / ".metaflow").exists()

    # Derive the run/task ids from what actually ran rather than hardcoding them
    # (task ids are not guaranteed to be a fixed value).
    run_id = (
        datastore_root / "CardRootRegressionFlow" / "latest_run"
    ).read_text().strip()
    # Card path layout: .../tasks/<task_id>/cards/<file>.html, so the task id is
    # two levels above the HTML file.
    task_id = card_files[0].parents[1].name

    # Reader side: the client/CLI resolves the card root from persisted `ds-root`
    # metadata. This proves the reader looks where the writer actually wrote.
    list_result = subprocess.run(
        [
            sys.executable,
            str(flow_file),
            "--quiet",
            "card",
            "list",
            "%s/start/%s" % (run_id, task_id),
            "--as-json",
        ],
        cwd=str(work_dir),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert list_result.returncode == 0, list_result.stdout + list_result.stderr
    listed_cards = json.loads(list_result.stdout)
    # The reader resolved the same task and found the single `blank` card the
    # writer produced -> write and read paths agree.
    assert listed_cards["pathspec"] == (
        "CardRootRegressionFlow/%s/start/%s" % (run_id, task_id)
    )
    assert [card["type"] for card in listed_cards["cards"]] == ["blank"]
