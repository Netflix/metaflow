import re


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


def compute_resource_attributes(decos, compute_deco, resource_defaults):
    """
    Compute resource values taking into account defaults, the values specified
    in the compute decorator (like @batch or @kubernetes) directly, and
    resources specified via @resources decorator.

    Returns a dictionary of resource attr -> value (str).
    """
    assert compute_deco is not None

    # Use the value from resource_defaults by default
    result = {k: v for k, v in resource_defaults.items()}

    for deco in decos:
        # If resource decorator is used
        if deco.name == "resources":
            for k, v in deco.attributes.items():
                # ..we use the larger of @resources and @batch attributes
                my_val = compute_deco.attributes.get(k)
                if not (my_val is None and v is None):
                    result[k] = str(max(int(my_val or 0), int(v or 0)))
            return result

    # If there is no resources decorator, values from compute_deco override
    # the defaults.
    for k, v in resource_defaults.items():
        if compute_deco.attributes.get(k):
            result[k] = str(compute_deco.attributes[k])

    return result
