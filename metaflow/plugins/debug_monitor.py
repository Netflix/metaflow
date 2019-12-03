from __future__ import print_function

import sys

from metaflow.sidecar_messages import MessageTypes, Message
from metaflow.monitor import Timer, deserialize_metric
from metaflow.monitor import MEASURE_TYPE, get_monitor_msg_type

class DebugMonitor(object):
    TYPE = 'debugMonitor'

    def __init__(self):
        self.logger('init')

    def count(self, count):
        pass

    def measure(self, timer):
        # type: (Timer) -> None
        self.logger('elapsed time for {}: {}'.
                    format(timer.name, str(timer.get_duration())))

    def gauge(self, gauge):
        pass

    def process_message(self, msg):
        # type: (Message) -> None
        self.logger('processing message %s' % str(msg.msg_type))
        msg_type = get_monitor_msg_type(msg)
        if msg_type == MEASURE_TYPE:
            timer = deserialize_metric(msg.payload.get('timer'))
            self.measure(timer)
        else:
            pass

    def shutdown(self):
        sys.stderr.flush()

    def logger(self, msg):
        print('local_monitor: %s' % msg, file=sys.stderr)
        sys.stderr.flush()
