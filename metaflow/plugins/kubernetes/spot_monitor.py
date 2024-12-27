import os
import sys
import time
import signal
from datetime import datetime
from threading import Thread

from metaflow.sidecar import MessageTypes
from metaflow.metaflow_current import current
from metaflow.metadata_provider import MetaDatum


class SpotTerminationMonitorSidecar(object):
    """
    Monitors AWS spot instance termination notices and propagates SIGTERM to the parent process
    when a termination notice is received.
    """

    METADATA_URL = "http://169.254.169.254/latest/meta-data/spot/termination-time"
    POLL_INTERVAL = 5  # seconds

    def __init__(self):
        self._thread = Thread(target=self._monitor_loop)
        self.is_alive = True
        self._metadata = None
        # Only start monitoring if we're on an AWS spot instance
        if self._is_aws_spot_instance():
            self._thread = Thread(target=self._monitor_loop)
            self._thread.start()

    def process_message(self, msg):
        if msg.msg_type == MessageTypes.SHUTDOWN:
            self.is_alive = False
        elif msg.msg_type == MessageTypes.MUST_SEND:
            self._metadata = msg.payload

    @classmethod
    def get_worker(cls):
        return cls

    def _is_aws_spot_instance(self):
        import requests

        self._record_termination_notice(datetime.now(datetime.timezone.utc).isoformat())
        try:
            response = requests.get(self.SPOT_METADATA_URL, timeout=1)
            # A 404 means we're on EC2 but not a spot instance
            # A timeout/connection error means we're not on EC2 at all
            return response.status_code != 404
        except (requests.exceptions.RequestException, requests.exceptions.Timeout):
            return False

    def _monitor_loop(self):
        import requests

        while self.is_alive:
            try:
                response = requests.get(self.METADATA_URL, timeout=1)
                if response.status_code == 200:
                    # Record the termination notice in metadata
                    self._record_termination_notice(response.text)

                    # Send SIGTERM to the parent process group
                    os.kill(os.getppid(), signal.SIGTERM)
                    break
            except (requests.exceptions.RequestException, requests.exceptions.Timeout):
                # Connection error or timeout - instance is fine, continue monitoring
                pass
            time.sleep(self.POLL_INTERVAL)

    def _record_termination_notice(self, termination_time):
        if self._metadata is None:
            sys.stderr.write(
                "Cannot record spot termination metadata: No metadata provider available\n"
            )
            return

        try:
            # Create metadata entries
            entries = [
                MetaDatum(
                    field="spot-termination-notice",
                    value=True,
                    type="aws-spot-termination",
                    tags=["attempt_id:{}".format(current.attempt)],
                ),
                MetaDatum(
                    field="spot-termination-time",
                    value=termination_time,
                    type="aws-spot-termination",
                    tags=["attempt_id:{}".format(current.attempt)],
                ),
                MetaDatum(
                    field="spot-termination-received-at",
                    value=datetime.now(datetime.timezone.utc).isoformat(),
                    type="aws-spot-termination",
                    tags=["attempt_id:{}".format(current.attempt)],
                ),
            ]

            # Register metadata
            self._metadata.register_metadata(
                run_id=current.run_id,
                step_name=current.step_name,
                task_id=current.task_id,
                metadata=entries,
            )
        except Exception as e:
            sys.stderr.write(
                "Failed to register spot termination metadata: %s\n" % str(e)
            )
