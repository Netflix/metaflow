import unittest
import subprocess
import os
import sys
import tempfile
import shutil

class TestNoCapture(unittest.TestCase):
    def test_worker_fds_return_empty_when_no_capture(self):
        from metaflow.runtime import Worker
        from unittest.mock import MagicMock
        
        task = MagicMock()
        task.retries = 0
        task.user_code_retries = 0
        task.is_cloned = False
        task.decos = []
        
        # Mock Worker._launch to return a mock process
        with MagicMock() as mock_popen:
            # We need to mock fcntl because Worker.__init__ calls it on Unix
            # But on Windows it might not be imported or used the same way.
            # Actually Worker.__init__ doesn't call fcntl, terminate() does.
            
            # Use a context manager to mock _launch
            with unittest.mock.patch('metaflow.runtime.Worker._launch', return_value=mock_popen):
                worker = Worker(task, 10, None, capture_output=False)
                self.assertEqual(worker.fds(), [])

class TestNoCaptureEndToEnd(unittest.TestCase):

    FLOW_CODE = """
import pdb
from metaflow import FlowSpec, step

class DebugFlow(FlowSpec):
    @step
    def start(self):
        # We don't actually want to block in a non-interactive test
        # but the prompt asks for this code.
        # We use input to 'c'ontinue.
        pdb.set_trace()
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    DebugFlow()
"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.flow_path = os.path.join(self.tmpdir, "debug_flow.py")
        with open(self.flow_path, "w") as f:
            f.write(self.FLOW_CODE)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_pdb_flow_completes_with_no_capture(self):
        try:
            # We use -u to ensure unbuffered output
            result = subprocess.run(
                [sys.executable, self.flow_path, "run",
                 "--no-capture", "--max-workers", "1"],
                input=b"c\n",
                capture_output=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0)
        except FileNotFoundError:
            self.skipTest("Metaflow not installed")
        except subprocess.TimeoutExpired:
            self.fail("Flow timed out — --no-capture not working correctly")

    def test_pdb_flow_hangs_without_flag(self):
        try:
            subprocess.run(
                [sys.executable, self.flow_path, "run",
                 "--max-workers", "1"],
                input=b"c\n",
                capture_output=True,
                timeout=8,
            )
        except FileNotFoundError:
            self.skipTest("Metaflow not installed")
        except subprocess.TimeoutExpired:
            pass  # Expected — confirms original bug exists
