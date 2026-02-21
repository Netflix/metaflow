import subprocess
import sys
from metaflow import Flow

def test_modifying_tags_does_not_change_created_at(tmp_path):

    # Run the flow via CLI
    subprocess.check_call(
        [sys.executable, "linear_flow.py", "run"]
    )

    runs = list(Flow("LinearFlow"))
    assert len(runs) > 0

    r = runs[0]
    before = r.created_at

    r.add_tag("status:deleted")

    after = r.created_at

    assert before == after