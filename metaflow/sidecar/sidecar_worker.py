from __future__ import print_function

import json
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


def deserialize_options(options_json):
    if options_json is None:
        return {}
    try:
        options = json.loads(options_json)
    except (TypeError, ValueError) as error:
        raise click.BadParameter("Sidecar options are not valid JSON: %s" % error)
    if not isinstance(options, dict):
        raise click.BadParameter("Sidecar options must decode to an object")
    return options


def instantiate_worker(sidecar_type, options):
    worker_class = sidecar_type.get_worker()
    if worker_class is None:
        return None
    if options:
        return worker_class(options=options)
    return worker_class()


@click.command(help="Initialize workers")
@tracing.cli("sidecar")
@click.argument("worker-type")
@click.argument("options-json", required=False)
def main(worker_type, options_json):
    sidecar_type = SIDECARS.get(worker_type)
    if sidecar_type is not None:
        worker = instantiate_worker(sidecar_type, deserialize_options(options_json))
        if worker is not None:
            process_messages(worker_type, worker)
        else:
            print(
                "[sidecar:%s] Sidecar does not have associated worker" % worker_type,
                file=sys.stderr,
            )
    else:
        print("Unrecognized sidecar_process: %s" % worker_type, file=sys.stderr)


if __name__ == "__main__":
    main()
