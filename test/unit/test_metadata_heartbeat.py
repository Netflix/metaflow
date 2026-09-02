import json
from unittest.mock import patch

import pytest
import requests

from metaflow.metadata_provider.heartbeat import HeartBeatException, MetadataHeartBeat


def _worker(url="http://localhost:8080/ping"):
    hb = MetadataHeartBeat()
    hb.hb_url = url
    return hb


def test_heartbeat_post_passes_a_timeout():
    # requests has no default timeout, so the post has to pass one explicitly.
    # Without it the call can block forever, _heartbeat never returns or raises,
    # and the retry/backoff in _ping never runs.
    hb = _worker()
    with patch("metaflow.metadata_provider.heartbeat.requests.post") as post:
        post.return_value.status_code = 200
        post.return_value.json.return_value = json.dumps({"wait_time_in_seconds": 10})
        hb._heartbeat()

    timeout = post.call_args.kwargs.get("timeout")
    assert timeout is not None, "heartbeat post must pass a timeout to requests"

    # accept either a single value or a (connect, read) pair, but both must be finite
    values = timeout if isinstance(timeout, tuple) else (timeout,)
    assert all(v is not None and v > 0 for v in values), timeout


def test_heartbeat_timeout_raises_heartbeat_exception():
    # The Timeout handler already exists in _heartbeat but is unreachable while
    # the post has no timeout. Once one is passed, a timing out request has to
    # surface as a HeartBeatException so _ping can back off.
    hb = _worker()
    with patch(
        "metaflow.metadata_provider.heartbeat.requests.post",
        side_effect=requests.exceptions.Timeout("read timed out"),
    ):
        with pytest.raises(HeartBeatException, match="Timeout"):
            hb._heartbeat()
