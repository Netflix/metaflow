"""Small set of helpers used inside ``FlowSpec`` step bodies.

Everything that used to live here (FlowDefinition, MetaflowCheck, the
@steps / @tag decorators, custom assertion helpers) is gone — tests are
now plain pytest modules with real FlowSpec subclasses.

What remains:

- ``ResumeFromHere`` / ``TestRetry`` — exceptions a step body can raise to
  trigger a resume or retry path.
- ``is_resumed`` / ``origin_run_id_for_resume`` — small accessors over
  ``metaflow.current``.
- ``try_to_get_card`` / ``retry_until_timeout`` — polling helpers for card
  tests where the card may be written asynchronously.
"""

from __future__ import annotations

import time

from metaflow import current
from metaflow.cards import get_cards
from metaflow.exception import MetaflowException
from metaflow.plugins.cards.exception import CardNotPresentException


class ResumeFromHere(MetaflowException):
    headline = "Resume requested"

    def __init__(self):
        super().__init__("This is not an error. Testing resume...")


class RetryRequested(MetaflowException):
    """Raise inside a step body to trigger @retry-driven re-execution.

    Named ``RetryRequested`` (not ``TestRetry``) so pytest doesn't mistake
    it for a test class via the ``Test`` prefix.
    """

    headline = "Testing retry"

    def __init__(self):
        super().__init__("This is not an error. Testing retry...")


# Backward-compat alias for any pre-migration step bodies that still import
# the old name. Will be removed once nothing references it.
TestRetry = RetryRequested


def is_resumed() -> bool:
    return current.origin_run_id is not None


def origin_run_id_for_resume() -> str | None:
    return current.origin_run_id


def truncate(var) -> str:
    s = str(var)
    if len(s) > 500:
        s = s[:500] + "..."
    return s


def retry_until_timeout(cb_fn, *args, timeout: float = 4, **kwargs):
    """Poll ``cb_fn`` until it returns a non-False value, or raise.

    Used inside step bodies for asynchronous card writes that don't
    appear immediately.
    """
    start = time.time()
    while True:
        val = cb_fn(*args, **kwargs)
        if val is not False:
            return val
        if time.time() - start > timeout:
            raise TimeoutError("Timeout waiting for callback to return non-False value")
        time.sleep(1)


def get_card_container(id=None):
    try:
        return get_cards(current.pathspec, id=id)
    except CardNotPresentException:
        return None


def try_to_get_card(id=None, timeout: float = 60):
    """Block until the card identified by ``id`` is available."""

    def _get(card_id):
        container = get_card_container(id=card_id)
        if container is None:
            return False
        return container[0]

    return retry_until_timeout(_get, id, timeout=timeout)
