import pytest

from metaflow import procpoll


@pytest.fixture
def darwin_poll(mocker):
    fake_select = mocker.patch.object(procpoll, "select")
    fake_select.kqueue.return_value.control.return_value = []
    return procpoll.DarwinProcPoll(), fake_select.kqueue.return_value


def test_darwin_poll_converts_milliseconds_to_seconds(darwin_poll):
    # ProcPoll.poll() takes milliseconds, like select.poll() on Linux, but
    # kqueue.control() takes seconds.
    poller, kqueue = darwin_poll

    list(poller.poll(1000))

    kqueue.control.assert_called_once_with(None, 100, 1.0)
