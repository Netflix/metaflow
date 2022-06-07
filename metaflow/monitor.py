import time

from contextlib import contextmanager

from .sidecar import SidecarSubProcess
from .sidecar_messages import Message, MessageTypes

COUNTER_TYPE = "COUNTER"
GAUGE_TYPE = "GAUGE"
TIMER_TYPE = "TIMER"


class NullMonitor(object):
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass

    def add_context(self, **kwargs):
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
    def __init__(self, monitor_type, env, **kwargs):
        # type: (str, str, str) -> None
        self.sidecar_process = None
        self.monitor_type = monitor_type
        self._context = {"env": env.get_environment_info()}
        # The additional keywords are added to the context for the monitor.
        # One use case is adding tags like flow_name etc.
        if kwargs:
            # Make sure we copy it as we don't want it to change
            self._context["user_context"] = dict(kwargs)

    def start(self):
        if self.sidecar_process is None:
            self.sidecar_process = SidecarSubProcess(self.monitor_type, self._context)

    def add_context(self, **kwargs):
        self._context["user_context"].update(kwargs)
        if self.sidecar_process is not None:
            msg = Message(MessageTypes.UPDATE_CONTEXT, self._context)
            self.sidecar_process.send(msg)

    @contextmanager
    def count(self, name):
        if self.sidecar_process is not None:
            counter = Counter(name)
            counter.increment()
            payload = {"counter": counter.serialize()}
            msg = Message(MessageTypes.LOG_EVENT, payload)
            yield
            self.sidecar_process.send(msg)
        else:
            yield

    @contextmanager
    def measure(self, name):
        if self.sidecar_process is not None:
            timer = Timer(name + "_timer")
            counter = Counter(name + "_counter")
            timer.start()
            counter.increment()
            yield
            timer.end()
            payload = {"counter": counter.serialize(), "timer": timer.serialize()}
            msg = Message(MessageTypes.LOG_EVENT, payload)
            self.sidecar_process.send(msg)
        else:
            yield

    def gauge(self, gauge):
        if self.sidecar_process is not None:
            payload = {"gauge": gauge.serialize()}
            msg = Message(MessageTypes.LOG_EVENT, payload)
            self.sidecar_process.send(msg)

    def terminate(self):
        if self.sidecar_process is not None:
            self.sidecar_process.kill()


class Metric(object):
    """
    Abstract base class
    """

    def __init__(self, metric_type, name, context=None):
        self._type = metric_type
        self._name = name
        self._context = context

    @property
    def metric_type(self):
        return self._type

    @property
    def name(self):
        return self._name

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, new_context):
        self._context = new_context

    @property
    def value(self):
        raise NotImplementedError()

    def serialize(self):
        # We purposefully do not serialize the environment as it can be large;
        # it will be transferred using a different mechanism and reset on the other
        # end.
        return {"_name": self._name, "_type": self._type}

    @classmethod
    def deserialize(cls, value):
        if value is None:
            return None
        metric_type = value.get("_type", "INVALID")
        metric_name = value.get("_name", None)
        metric_cls = _str_type_to_type.get(metric_type, None)
        if metric_cls:
            return metric_cls.deserialize(metric_name, value)
        else:
            raise NotImplementedError("Metric class %s is not supported" % metric_type)


class Timer(Metric):
    def __init__(self, name, env=None):
        super(Timer, self).__init__(TIMER_TYPE, name, env)
        self._start = 0
        self._end = 0

    def start(self, now=None):
        if now is None:
            now = time.time()
        self._start = now

    def end(self, now=None):
        if now is None:
            now = time.time()
        self._end = now

    @property
    def duration(self):
        return self._end - self._start

    @property
    def value(self):
        return self.duration * 1000

    def serialize(self):
        parent_ser = super(Timer, self).serialize()
        parent_ser["_start"] = self._start
        parent_ser["_end"] = self._end
        return parent_ser

    @classmethod
    def deserialize(cls, metric_name, value):
        t = Timer(metric_name)
        t.start(value.get("_start", 0))
        t.end(value.get("_end", 0))
        return t


class Counter(Metric):
    def __init__(self, name, env=None):
        super(Counter, self).__init__(COUNTER_TYPE, name, env)
        self._count = 0

    def increment(self):
        self._count += 1

    def set_count(self, count):
        self._count = count

    @property
    def value(self):
        return self._count

    def serialize(self):
        parent_ser = super(Counter, self).serialize()
        parent_ser["_count"] = self._count
        return parent_ser

    @classmethod
    def deserialize(cls, metric_name, value):
        c = Counter(metric_name)
        c.set_count(value.get("_count", 0))
        return c


class Gauge(Metric):
    def __init__(self, name, env=None):
        super(Gauge, self).__init__(GAUGE_TYPE, name, env)
        self._value = 0

    def set_value(self, val):
        self._value = val

    def increment(self):
        self._value += 1

    @property
    def value(self):
        return self._value

    def serialize(self):
        parent_ser = super(Gauge, self).serialize()
        parent_ser["_value"] = self._value

    @classmethod
    def deserialize(cls, metric_name, value):
        g = Gauge(metric_name)
        g.set_value(value.get("_value", 0))
        return g


_str_type_to_type = {COUNTER_TYPE: Counter, GAUGE_TYPE: Gauge, TIMER_TYPE: Timer}
