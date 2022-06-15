import threading
import time
import os
from contextlib import contextmanager


@contextmanager
def profile(label, stats_dict=None):
    # TODO revert this
    def nprint(*args):
        pass

    start = time.time()
    if stats_dict is None:
        nprint(
            "%s %s %.4f PROFILE: %s starting"
            % (os.getpid(), threading.get_ident(), start, label)
        )
    yield
    end = time.time()
    took = int((end - start) * 1000)
    if stats_dict is None:
        slow_marker = ""
        if took > 200:
            slow_marker = " GT200"
        elif took > 150:
            slow_marker = " GT150"
        elif took > 100:
            slow_marker = " GT100"
        elif took > 50:
            slow_marker = " GT50"
        elif took > 25:
            slow_marker = " GT25"

        nprint(
            "%s %s %.4f PROFILE: %s completed in %dms%s"
            % (os.getpid(), threading.get_ident(), end, label, took, slow_marker)
        )
    else:
        stats_dict[label] = stats_dict.get(label, 0) + took
