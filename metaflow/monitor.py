import time

from contextlib import contextmanager

from .sidecar import SidecarSubProcess
from .sidecar_messages import Message, MessageTypes

COUNTER_TYPE = "COUNTER"
GAUGE_TYPE = "GAUGE"
MEASURE_TYPE = "MEASURE"
TIMER_TYPE = "TIMER"

class NullMonitor(object):

    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass

    @contextmanager
    def count(self, name):
        yield

    @contextmanager
    def measure(self, name):
        yield

    def gauge(self, gauge):
        pass

    def terminate(self):
        pass

class Monitor(NullMonitor):

    def __init__(self, monitor_type, env, flow_name):
        # type: (str) -> None
        self.sidecar_process = None
        self.monitor_type = monitor_type
        self.env_info = env.get_environment_info()
        self.env_info["flow_name"] = flow_name

    def start(self):
        if self.sidecar_process is None:
            self.sidecar_process = SidecarSubProcess(self.monitor_type)

    @contextmanager
    def count(self, name):
        if self.sidecar_process is not None:
            counter = Counter(name, self.env_info)
            counter.increment()
            payload = {
                'counter': counter.to_dict()
            }
            msg = Message(MessageTypes.LOG_EVENT, payload)
            yield
            self.sidecar_process.msg_handler(msg)
        else:
            yield

    @contextmanager
    def measure(self, name):
        if self.sidecar_process is not None:
            timer = Timer(name + "_timer", self.env_info)
            counter = Counter(name + "_counter", self.env_info)
            timer.start()
            counter.increment()
            yield
            timer.end()
            payload = {
                'counter': counter.to_dict(),
                'timer': timer.to_dict()
            }
            msg = Message(MessageTypes.LOG_EVENT, payload)
            self.sidecar_process.msg_handler(msg)
        else:
            yield

    def gauge(self, gauge):
        if self.sidecar_process is not None:
            payload = {
                'gauge': gauge.to_dict()
            }
            msg = Message(MessageTypes.LOG_EVENT, payload)
            self.sidecar_process.msg_handler(msg)

    def terminate(self):
        if self.sidecar_process is not None:
            self.sidecar_process.kill()


class Metric(object):
    """
        Abstract base class
    """

    def __init__(self, type, env):
        self._env = env
        self._type = type

    @property
    def name(self):
        raise NotImplementedError()

    @property
    def flow_name(self):
        return self._env['flow_name']

    @property
    def env(self):
        return self._env

    @property
    def value(self):
        raise NotImplementedError()

    def set_env(self, env):
        self._env = env

    def to_dict(self):
        return {
            '_env': self._env,
            '_type': self._type,
        }


class Timer(Metric):
    def __init__(self, name, env):
        super(Timer, self).__init__(TIMER_TYPE, env)
        self._name = name
        self._start = 0
        self._end = 0

    @property
    def name(self):
        return self._name

    def start(self):
        self._start = time.time()

    def end(self):
        self._end = time.time()

    def set_start(self, start):
        self._start = start

    def set_end(self, end):
        self._end = end

    def get_duration(self):
        return self._end - self._start

    @property
    def value(self):
        return (self._end - self._start) * 1000

    def to_dict(self):
        parent_dict = super(Timer, self).to_dict()
        parent_dict['_name'] = self.name
        parent_dict['_start'] = self._start
        parent_dict['_end'] = self._end
        return parent_dict


class Counter(Metric):
    def __init__(self, name, env):
        super(Counter, self).__init__(COUNTER_TYPE, env)
        self._name = name
        self._count = 0

    @property
    def name(self):
        return self._name

    def increment(self):
        self._count += 1

    def set_count(self, count):
        self._count = count

    @property
    def value(self):
        return self._count

    def to_dict(self):
        parent_dict = super(Counter, self).to_dict()
        parent_dict['_name'] = self.name
        parent_dict['_count'] = self._count
        return parent_dict


class Gauge(Metric):
    def __init__(self, name, env):
        super(Gauge, self).__init__(GAUGE_TYPE, env)
        self._name = name
        self._value = 0

    @property
    def name(self):
        return self._name

    def set_value(self, val):
        self._value = val

    def increment(self):
        self._value += 1

    @property
    def value(self):
        return self._value

    def to_dict(self):
        parent_dict = super(Gauge, self).to_dict()
        parent_dict['_name'] = self.name
        parent_dict['_value'] = self.value
        return parent_dict


def deserialize_metric(metrics_dict):
    if metrics_dict is None:
        return

    type = metrics_dict.get('_type')
    name = metrics_dict.get('_name')
    if type == COUNTER_TYPE:
        try:
            counter = Counter(name, None)
            counter.set_env(metrics_dict.get('_env'))
        except Exception as ex:
            return

        counter.set_count(metrics_dict.get('_count'))
        return counter
    elif type == TIMER_TYPE:
        timer = Timer(name, None)
        timer.set_start(metrics_dict.get('_start'))
        timer.set_end(metrics_dict.get('_end'))
        timer.set_env(metrics_dict.get('_env'))
        return timer
    elif type == GAUGE_TYPE:
        gauge = Gauge(name, None)
        gauge.set_env(metrics_dict.get('_env'))
        gauge.set_value(metrics_dict.get('_value'))
        return gauge
    else:
        raise NotImplementedError("UNSUPPORTED MESSAGE TYPE IN MONITOR")


def get_monitor_msg_type(msg):
    if msg.payload.get('gauge') is not None:
        return GAUGE_TYPE
    if msg.payload.get('counter') is not None:
        if msg.payload.get('timer') is not None:
            return MEASURE_TYPE
        return COUNTER_TYPE