import math
import random
import re
import time


def get_docker_registry(image_uri):
    """
    Explanation:
        (.+?(?:[:.].+?)\/)? - [GROUP 0] REGISTRY
            .+?                 - A registry must start with at least one character
            (?:[:.].+?)\/       - A registry must have ":" or "." and end with "/"
            ?                   - Make a registry optional
        (.*?)               - [GROUP 1] REPOSITORY
            .*?                 - Get repository name until separator
        (?:[@:])?           - SEPARATOR
            ?:                  - Don't capture separator
            [@:]                - The separator must be either "@" or ":"
            ?                   - The separator is optional
        ((?<=[@:]).*)?      - [GROUP 2] TAG / DIGEST
            (?<=[@:])           - A tag / digest must be preceeded by "@" or ":"
            .*                  - Capture rest of tag / digest
            ?                   - A tag / digest is optional
    Examples:
        image
            - None
            - image
            - None
        example/image
            - None
            - example/image
            - None
        example/image:tag
            - None
            - example/image
            - tag
        example.domain.com/example/image:tag
            - example.domain.com/
            - example/image
            - tag
        123.123.123.123:123/example/image:tag
            - 123.123.123.123:123/
            - example/image
            - tag
        example.domain.com/example/image@sha256:45b23dee0
            - example.domain.com/
            - example/image
            - sha256:45b23dee0
    """

    pattern = re.compile(r"^(.+?(?:[:.].+?)\/)?(.*?)(?:[@:])?((?<=[@:]).*)?$")
    registry, repository, tag = pattern.match(image_uri).groups()
    if registry is not None:
        registry = registry.rstrip("/")
    return registry


def retry(
    function=None,
    *,
    exceptions=tuple(),
    exception_handler=lambda x: True,
    deadline_seconds=None,
    max_backoff=None,
):
    """
    A factory method which returns a truncated exponential backoff retry decorator.

    For deadline_seconds and max_backoff see
    https://cloud.google.com/storage/docs/retry-strategy#exponential-backoff.

    Args:
        function: Included in the design pattern to allow the decorator to run
                  with and without parentheses (@deco and @deco(params))
        exceptions: A single exception or a tuple of exceptions.
        exception_handler: A filter function, for which True indicates that a retry
                           should take place, and False indicates that the exception
                           should be raised.
    """

    def decorator(f):
        """
        Implements truncated exponential backoff from
        https://cloud.google.com/storage/docs/retry-strategy#exponential-backoff
        """
        from functools import wraps

        @wraps(f)
        def wrapper(*args, **kwargs):
            deadline = time.time() + deadline_seconds
            retry_number = 0

            while True:
                try:
                    result = f(*args, **kwargs)
                    return result
                except exceptions as e:
                    if exception_handler(e):
                        current_t = time.time()
                        backoff_delay = min(
                            math.pow(2, retry_number) + random.random(), max_backoff
                        )
                        if current_t + backoff_delay < deadline:
                            time.sleep(backoff_delay)
                            retry_number += 1
                            continue  # retry again
                        else:
                            raise
                    else:
                        raise

        return wrapper

    if function:
        return decorator(function)
    return decorator
