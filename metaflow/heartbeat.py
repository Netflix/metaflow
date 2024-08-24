from metaflow.plugins.datatools.s3.s3util import get_s3_client
from urllib.parse import urlparse
import os
import time
from datetime import datetime, timedelta, timezone
import sys, signal


def monitor_heartbeat(heartbeat_bucket, heartbeat_filename, main_pid, timeout=60):
    s3, _ = get_s3_client()
    heartbeat_key = f"metaflow/heartbeats/{heartbeat_filename}"

    while True:
        try:
            response = s3.get_object(Bucket=heartbeat_bucket, Key=heartbeat_key)
            content = response["Body"].read().decode("utf-8").strip()
            if "tombstone" in content:
                print("Tombstone detected! Terminating...")
                os.kill(main_pid, signal.SIGTERM)
                sys.exit(1)

            timestamp_str = content.split(",")[1]
            timestamp = datetime.strptime(
                timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - timestamp > timedelta(seconds=timeout):
                print("Heartbeat timeout! Terminating...")
                os.kill(main_pid, signal.SIGTERM)
                sys.exit(1)
            else:
                print("Heartbeat is fresh.")
        except Exception as e:
            print(f"Error reading heartbeat: {e}")
            os.kill(main_pid, signal.SIGTERM)
            sys.exit(1)

        time.sleep(int(timeout / 4))


if __name__ == "__main__":
    main_pid = int(sys.argv[1])
    # Read parameters from environment variables or command line arguments
    heartbeat_bucket = urlparse(os.getenv("METAFLOW_DATATOOLS_S3ROOT")).netloc
    heartbeat_filename = f"{os.getenv('MF_PATHSPEC')}/{os.getenv('MF_ATTEMPT')}"

    if not heartbeat_filename or not heartbeat_bucket:
        print("Missing required environment variables.")
        os.kill(main_pid, signal.SIGTERM)
        sys.exit(1)

    monitor_heartbeat(heartbeat_bucket, heartbeat_filename, main_pid)
