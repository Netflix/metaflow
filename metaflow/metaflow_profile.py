import time

from contextlib import contextmanager

from .metaflow_config import PROFILE_FROM_START

init_time = None


if PROFILE_FROM_START:

    def from_start(msg: str):
        global init_time
        if init_time is None:
            init_time = time.time()
        print("From start: %s took %dms" % (msg, int((time.time() - init_time) * 1000)))

else:

    def from_start(_msg: str):
        pass


@contextmanager
def profile(label, stats_dict=None):
    if stats_dict is None:
        print("PROFILE: %s starting" % label)
    start = time.time()
    yield
    took = int((time.time() - start) * 1000)
    if stats_dict is None:
        print("PROFILE: %s completed in %dms" % (label, took))
    else:
        stats_dict[label] = stats_dict.get(label, 0) + took
