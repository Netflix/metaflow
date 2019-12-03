from __future__ import print_function

import os
import sys
import click
import traceback


# add module to python path if not already present
myDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.split(myDir)[0]
sys.path.insert(0, parentDir)

from metaflow.sidecar_messages import MessageTypes, deserialize
from metaflow.plugins import SIDECAR


class WorkershutdownError(Exception):
    """raised when terminating sidecar"""
    pass


def process_messages(worker):
    while True:
        try:
            msg = sys.stdin.readline().strip()
            if msg:
                parsed_msg = deserialize(msg)
                if parsed_msg.msg_type == MessageTypes.SHUTDOWN:
                    raise WorkershutdownError()
                else:
                    worker.process_message(parsed_msg)
            else:
                raise WorkershutdownError()
        except WorkershutdownError:
            worker.shutdown()
            break
        except Exception as e:  # todo handle other possible exceptions gracefully
            print(traceback.format_exc())
            worker.shutdown()
            break


@click.command(help="Initialize workers")
@click.argument('worker-type')
def main(worker_type):

    worker_process = SIDECAR.get(worker_type)

    if worker_process is not None:
        process_messages(worker_process())
    else:
        print("UNRECOGNIZED WORKER: %s" % worker_type, file=sys.stderr)


if __name__ == "__main__":
    main()
