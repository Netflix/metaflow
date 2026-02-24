from metaflow import Runner
import os

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def test_retry_flow():
    with Runner(os.path.join(FLOWS_DIR, "retry_flow.py")).run() as running:
        assert running.run.successful
        assert running.run.data.message == "Retry flow successful"
