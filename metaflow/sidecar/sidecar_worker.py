from __future__ import print_function

import os
import sys

import traceback


# add metaflow module to python path if not already present
myDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.split(os.path.split(myDir)[0])[0]
sys.path.insert(0, parentDir)

from metaflow.sidecar import Message, MessageTypes
from metaflow.plugins import SIDECARS
from metaflow._vendor import click
import metaflow.tracing as tracing


def process_messages(worker_type, worker):
    while True:
        try:
            msg = sys.stdin.readline().strip()
            if msg:
                parsed_msg = Message.deserialize(msg)
                if parsed_msg.msg_type == MessageTypes.INVALID:
                    print(
                        "[sidecar:%s] Invalid message -- skipping: %s"
                        % (worker_type, str(msg))
                    )
                    continue
                else:
                    worker.process_message(parsed_msg)
                    if parsed_msg.msg_type == MessageTypes.SHUTDOWN:
                        break
            else:
                break

        except:  # todo handle other possible exceptions gracefully
            print(
                "[sidecar:%s]: %s" % (worker_type, traceback.format_exc()),
                file=sys.stderr,
            )
            break
    try:
        worker.shutdown()
    except:
        pass


@click.command(help="Initialize workers")
@tracing.cli("sidecar")
@click.argument("worker-type")
def main(worker_type):
    sidecar_type = SIDECARS.get(worker_type)
    if sidecar_type is not None:
        worker_class = sidecar_type.get_worker()
        if worker_class is not None:
            process_messages(worker_type, worker_class())
        else:
            print(
                "[sidecar:%s] Sidecar does not have associated worker" % worker_type,
                file=sys.stderr,
            )
    else:
        print("Unrecognized sidecar_process: %s" % worker_type, file=sys.stderr)


if __name__ == "__main__":
    main()
