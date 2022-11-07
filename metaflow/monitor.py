import time

from contextlib import contextmanager

from metaflow.sidecar import Message, MessageTypes, Sidecar

COUNTER_TYPE = "COUNTER"
GAUGE_TYPE = "GAUGE"
TIMER_TYPE = "TIMER"


class NullMonitor(object):
    TYPE = "nullSidecarMonitor"

    def __init__(self, *args, **kwargs):
        # Currently passed flow and env as kwargs
        self._sidecar = Sidecar(self.TYPE)

    def start(self):
        return self._sidecar.start()

    def terminate(self):
        return self._sidecar.terminate()

    def send(self, msg):
        # Arbitrary message sending. Useful if you want to override some different
        # types of messages.
        self._sidecar.send(msg)

    @contextmanager
    def count(self, name):
        if self._sidecar.is_active:
            counter = Counter(name)
            counter.increment()
            payload = {"counter": counter.serialize()}
            msg = Message(MessageTypes.BEST_EFFORT, payload)
            yield
            self._sidecar.send(msg)
        else:
            yield

    @contextmanager
    def measure(self, name):
        if self._sidecar.is_active:
            timer = Timer(name + "_timer")
            counter = Counter(name + "_counter")
            timer.start()
            counter.increment()
            yield
            timer.end()
            payload = {"counter": counter.serialize(), "timer": timer.serialize()}
            msg = Message(MessageTypes.BEST_EFFORT, payload)
            self._sidecar.send(msg)
        else:
            yield

    def gauge(self, gauge):
        if self._sidecar.is_active:
            payload = {"gauge": gauge.serialize()}
            msg = Message(MessageTypes.BEST_EFFORT, payload)
            self._sidecar.send(msg)

    @classmethod
    def get_worker(cls):
        return None


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
        # We purposefully do not serialize the context as it can be large;
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
