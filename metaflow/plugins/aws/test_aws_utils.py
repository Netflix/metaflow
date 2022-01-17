import pytest

from .aws_utils import retry


@pytest.mark.parametrize("use_parentheses", (True, False))
def test_retry(use_parentheses):
    def my_retry(function=None):
        return retry(
            function=function,
            exception=lambda: KeyError,
            exception_handler=lambda keyerror: str(keyerror) == "'foo'",
            deadline_seconds=3,  # 1 retry is a max of 2 seconds
            max_backoff=2,
        )

    my_retry = my_retry() if use_parentheses else my_retry

    @my_retry
    def my_bad_func(key):
        """Always throws KeyError"""
        my_bad_func.counter += 1
        {}[key]  # <-- KeyError

    # Case 1: exception_handler returns True, so do retry
    my_bad_func.counter = 0
    with pytest.raises(KeyError):
        my_bad_func("foo")  # "foo" triggers a retry
    assert my_bad_func.counter > 1

    # Case 2: exception_handler returns False, so don't retry
    my_bad_func.counter = 0
    with pytest.raises(KeyError):
        my_bad_func("bar")  # "bar" does not trigger a retry
    assert my_bad_func.counter == 1
