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
