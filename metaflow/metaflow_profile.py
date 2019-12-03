import time

from contextlib import contextmanager

@contextmanager
def profile(label, stats_dict=None):
    if stats_dict is None:
        print('PROFILE: %s starting' % label)
    start = time.time()
    yield
    took = int((time.time() - start) * 1000)
    if stats_dict is None:
        print('PROFILE: %s completed in %dms' % (label, took))
    else:
        stats_dict[label] = stats_dict.get(label, 0) + took
