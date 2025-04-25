import os
import sys
import time
import signal
import requests
import subprocess
from multiprocessing import Process
from datetime import datetime, timedelta, timezone
from metaflow.sidecar import MessageTypes


class SpotTerminationMonitorSidecar(object):
    EC2_TYPE_URL = "http://169.254.169.254/latest/meta-data/instance-life-cycle"
    METADATA_URL = "http://169.254.169.254/latest/meta-data/spot/termination-time"
    TOKEN_URL = "http://169.254.169.254/latest/api/token"
    POLL_INTERVAL = 5  # seconds

    def __init__(self):
        self.is_alive = True
        self._process = None
        self._token = None
        self._token_expiry = 0

        self.termination_time = None
        # Due to nesting, os.getppid is not reliable for fetching the main task pid
        self.main_pid = int(os.getenv("MF_MAIN_PID", os.getppid()))

        if self._is_aws_spot_instance():
            self._process = Process(target=self._monitor_loop)
            self._process.start()

    def process_message(self, msg):
        if msg.msg_type == MessageTypes.SHUTDOWN:
            self.is_alive = False
            if self._process:
                self._process.terminate()

    @classmethod
    def get_worker(cls):
        return cls

    def _get_imds_token(self):
        current_time = time.time()
        if current_time >= self._token_expiry - 60:  # Refresh 60s before expiry
            try:
                response = requests.put(
                    url=self.TOKEN_URL,
                    headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"},
                    timeout=1,
                )
                if response.status_code == 200:
                    self._token = response.text
                    self._token_expiry = current_time + 240  # Slightly less than TTL
            except requests.exceptions.RequestException:
                pass
        return self._token

    def _make_ec2_request(self, url, timeout):
        token = self._get_imds_token()
        headers = {"X-aws-ec2-metadata-token": token} if token else {}
        response = requests.get(url=url, headers=headers, timeout=timeout)
        return response

    def _is_aws_spot_instance(self):
        try:
            response = self._make_ec2_request(url=self.EC2_TYPE_URL, timeout=1)
            return response.status_code == 200 and response.text == "spot"
        except (requests.exceptions.RequestException, requests.exceptions.Timeout):
            return False

    def _try_sending_termination_signal(self):
        # wait for 100 seconds before the promised termination time of the spot instance before sending a SIGALRM
        if (
            datetime.now(timezone.utc) + timedelta(0, self.POLL_INTERVAL + 100)
        ) < datetime.strptime(self.termination_time, "%Y-%m-%dT%H:%M:%SZ"):
            return False
        else:
            os.kill(self.main_pid, signal.SIGALRM)
            return True

    def _monitor_spot_termination(self):
        if self.termination_time is not None:
            return self.termination_time

        try:
            response = self._make_ec2_request(url=self.METADATA_URL, timeout=1)
            if response.status_code == 200:
                self.termination_time = response.text
                self._emit_termination_metadata(self.termination_time)
                os.kill(self.main_pid, signal.SIGTERM)
        except (requests.exceptions.RequestException, requests.exceptions.Timeout):
            pass

        return self.termination_time

    def _monitor_loop(self):
        while self.is_alive:
            terminates_at = self._monitor_spot_termination()

            sent_signal = False
            if terminates_at is not None:
                sent_signal = self._try_sending_termination_signal()

            if sent_signal:
                break
            time.sleep(self.POLL_INTERVAL)

    def _emit_termination_metadata(self, termination_time):
        flow_filename = os.getenv("METAFLOW_FLOW_FILENAME")
        pathspec = os.getenv("MF_PATHSPEC")
        _, run_id, step_name, task_id = pathspec.split("/")
        retry_count = os.getenv("MF_ATTEMPT")

        with open("/tmp/spot_termination_notice", "w") as fp:
            fp.write(termination_time)

        command = [
            sys.executable,
            f"/metaflow/{flow_filename}",
            "spot-metadata",
            "record",
            "--run-id",
            run_id,
            "--step-name",
            step_name,
            "--task-id",
            task_id,
            "--termination-notice-time",
            termination_time,
            "--tag",
            "attempt_id:{}".format(retry_count),
        ]

        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Failed to record spot termination metadata: {result.stderr}")
