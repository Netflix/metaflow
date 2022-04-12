import os
import json
import random
import string
import zlib
from itertools import dropwhile, islice

from metaflow.util import to_bytes


def _token_generator(token_prefix):
    for i in range(10000):
        prefix = "%s-%d-" % (token_prefix, i)
        # we need to use a consistent hash here, which is why
        # random.seed(prefix) or random.seed(hash(prefix)) won't work
        random.seed(zlib.adler32(to_bytes(prefix)))
        yield prefix + "".join(random.sample(string.ascii_lowercase, 4))


def _makedirs(path):
    # this is for python2 compatibility.
    # Python3 has os.makedirs(exist_ok=True).
    try:
        os.makedirs(path)
    except OSError as x:
        if x.errno == 17:
            return
        else:
            raise


def _load_config(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    else:
        return {}


def _path(token_prefix):
    home = os.environ.get("METAFLOW_HOME", "~/.metaflowconfig")
    return os.path.expanduser("%s/%s" % (home, token_prefix))


def new_token(token_prefix, prev_token=None):
    if prev_token is None:
        for token in _token_generator(token_prefix):
            return token
    else:
        it = dropwhile(lambda x: x != prev_token, _token_generator(token_prefix))
        for _ in it:
            return next(it)
        else:
            return None


def load_token(token_prefix):
    config = _load_config(_path(token_prefix))
    return config.get("production_token")


def store_token(token_prefix, token):
    path = _path(token_prefix)
    config = _load_config(path)
    config["production_token"] = token
    _makedirs(os.path.dirname(path))
    with open(path, "w") as f:
        json.dump(config, f)
