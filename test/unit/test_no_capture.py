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
            # mock_popen.stdout and mock_popen.stderr will be mocked to return None in capture_output=False case
            # but we patch _launch to return a mock process. 
            # In the real code, capture_output=False sets stdout=None, stderr=None in Popen.
            
            with unittest.mock.patch('metaflow.runtime.Worker._launch', return_value=mock_popen):
                worker = Worker(task, 10, None, capture_output=False)
                # Ensure the mock process handles stdout/stderr as None
                worker._proc.stdout = None
                worker._proc.stderr = None
                self.assertEqual(worker.fds(), [])

class TestNoCaptureEndToEnd(unittest.TestCase):

    FLOW_CODE = """
import sys
from metaflow import FlowSpec, step

class DebugFlow(FlowSpec):
    @step
    def start(self):
        # Use breakpoint() with a custom hook instead of pdb.set_trace()
        # so it works reliably without a tty in CI environments
        completed = []
        original = sys.breakpointhook if hasattr(sys, 'breakpointhook') else None
        def noop_hook(*args, **kwargs):
            completed.append(True)
        sys.breakpointhook = noop_hook
        breakpoint()
        assert completed, "breakpoint() was not called"
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

    def test_no_capture_flow_completes(self):
        \"\"\"Flow with breakpoint() completes successfully with --no-capture.\"\"\"
        result = subprocess.run(
            [sys.executable, self.flow_path, "run",
             "--no-capture", "--max-workers", "1"],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 1 and b"ImportError" in result.stderr:
            self.skipTest("Metaflow not installed in this environment")
        self.assertEqual(
            result.returncode, 0,
            msg=(
                f"Flow failed with --no-capture.\\n"
                f"STDOUT:\\n{result.stdout.decode()}\\n"
                f"STDERR:\\n{result.stderr.decode()}"
            ),
        )

    def test_normal_capture_still_works(self):
        \"\"\"Normal run without --no-capture still completes (regression check).\"\"\"
        # Remove breakpoint from flow for this test
        normal_flow = self.FLOW_CODE.replace("breakpoint()", "pass")
        normal_path = os.path.join(self.tmpdir, "normal_flow.py")
        with open(normal_path, "w") as f:
            f.write(normal_flow)

        result = subprocess.run(
            [sys.executable, normal_path, "run", "--max-workers", "1"],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 1 and b"ImportError" in result.stderr:
            self.skipTest("Metaflow not installed in this environment")
        self.assertEqual(result.returncode, 0)
