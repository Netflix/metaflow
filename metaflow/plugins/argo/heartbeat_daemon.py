import os
import sys
from time import sleep
from metaflow.metadata.heartbeat import MetadataHeartBeat, HB_URL_KEY
from metaflow.sidecar.sidecar_messages import Message, MessageTypes
from metaflow.metaflow_config import SERVICE_URL


def main(flow_name, run_id):
    # Reuse the metadataHeartbeat mechanism for Argo Daemon Containers as well
    daemon = MetadataHeartBeat()
    payload = {
        HB_URL_KEY: os.path.join(
            SERVICE_URL, f"flows/{flow_name}/runs/{run_id}/heartbeat"
        )
    }
    msg = Message(MessageTypes.BEST_EFFORT, payload)
    # start heartbeating
    daemon.process_message(msg)
    while True:
        print("is daemon alive:", daemon.req_thread.is_alive())
        print("sleeping for 10 seconds (does not affect heartbeats)...")
        sleep(10)


if __name__ == "__main__":
    flow_name, run_id = sys.argv[1:]
    main(flow_name, run_id)
