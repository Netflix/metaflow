import re

from metaflow.exception import MetaflowException


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
            (?<=[@:])           - A tag / digest must be preceded by "@" or ":"
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


def compute_resource_attributes(decos, compute_deco, resource_defaults):
    """
    Compute resource values taking into account defaults, the values specified
    in the compute decorator (like @batch or @kubernetes) directly, and
    resources specified via @resources decorator.

    Returns a dictionary of resource attr -> value (str).
    """
    assert compute_deco is not None

    # Use the value from resource_defaults by default (don't use None)
    result = {k: v for k, v in resource_defaults.items() if v is not None}

    for deco in decos:
        # If resource decorator is used
        if deco.name == "resources":
            for k, v in deco.attributes.items():
                my_val = compute_deco.attributes.get(k)
                # We use the non None value if there is only one or the larger value
                # if they are both non None. Note this considers "" to be equivalent to
                # the value zero.
                if my_val is None and v is None:
                    continue
                if my_val is not None and v is not None:
                    try:
                        # Use Decimals to compare and convert to string here so
                        # that numbers that can't be exactly represented as
                        # floats (e.g. 0.8) still look "nice". We don't care
                        # about precision more that .001 for resources anyway.
                        result[k] = str(max(float(my_val or 0), float(v or 0)))
                    except ValueError:
                        # Here we don't have ints, so we compare the value and raise
                        # an exception if not equal
                        if my_val != v:
                            raise MetaflowException(
                                "'resources' and compute decorator have conflicting "
                                "values for '%s'. Please use consistent values or "
                                "specify this resource constraint once" % k
                            )
                elif my_val is not None:
                    result[k] = str(my_val or "0")
                else:
                    result[k] = str(v or "0")
            return result

    # If there is no resources decorator, values from compute_deco override
    # the defaults.
    for k in resource_defaults:
        if compute_deco.attributes.get(k) is not None:
            result[k] = str(compute_deco.attributes[k] or "0")

    return result
