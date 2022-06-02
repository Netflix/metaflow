from __future__ import print_function

import os
import sys

import traceback


# add module to python path if not already present
myDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.split(myDir)[0]
sys.path.insert(0, parentDir)

from metaflow.sidecar_messages import Message, MessageTypes
from metaflow.plugins import SIDECARS
from metaflow._vendor import click


def process_messages(worker):
    while True:
        try:
            msg = sys.stdin.readline().strip()
            if msg:
                parsed_msg = Message.deserialize(msg)
                if parsed_msg.msg_type == MessageTypes.INVALID:
                    print("Invalid message -- skipping")
                    continue
                else:
                    worker.process_message(parsed_msg)
                    if parsed_msg.msg_type == MessageTypes.SHUTDOWN:
                        break
            else:
                break

        except:  # todo handle other possible exceptions gracefully
            print(traceback.format_exc(), file=sys.stderr)
            break
    try:
        worker.shutdown()
    except:
        pass


@click.command(help="Initialize workers")
@click.argument("worker-type")
def main(worker_type):
    worker_process = SIDECARS.get(worker_type)

    if worker_process is not None:
        process_messages(worker_process())
    else:
        print("Unrecognized worker: %s" % worker_type, file=sys.stderr)


if __name__ == "__main__":
    main()
