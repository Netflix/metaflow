import json
import os
import secrets
import string



_TOKEN_LENGTH = 16
_TOKEN_ALPHABET = string.ascii_lowercase + string.digits


def _generate_token(token_prefix):
    suffix = "".join(secrets.choice(_TOKEN_ALPHABET) for _ in range(_TOKEN_LENGTH))
    return "%s-%s" % (token_prefix, suffix)


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
    # TODO make this a MF config variable
    if os.environ.get("METAFLOW_TOKEN_HOME"):
        home = os.environ.get("METAFLOW_TOKEN_HOME")
    else:
        home = os.environ.get("METAFLOW_HOME", "~/.metaflowconfig")
    return os.path.expanduser("%s/%s" % (home, token_prefix))


def new_token(token_prefix, prev_token=None):
    # prev_token was used for deterministic sequence iteration in the old
    # implementation. With cryptographic randomness a fresh token can always
    # be generated, so the parameter is retained only for API compatibility.
    return _generate_token(token_prefix)


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
