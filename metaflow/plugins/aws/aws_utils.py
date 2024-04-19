import re
import requests

from metaflow.exception import MetaflowException


def get_ec2_instance_metadata():
    """
    Fetches the EC2 instance metadata through AWS instance metadata service

    Returns either an empty dictionary, or one with the keys
        - ec2-instance-id
        - ec2-instance-type
        - ec2-region
        - ec2-availability-zone
    """
    meta = {}
    # Capture AWS instance identity metadata. This is best-effort only since
    # access to this end-point might be blocked on AWS and not available
    # for non-AWS deployments.
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
    # Set a very aggressive timeout, as the communication is happening in the same subnet,
    # there should not be any significant delay in the response.
    # Having a long default timeout here introduces unnecessary delay in launching tasks when the
    # instance is unreachable.
    timeout = (1, 10)
    token = None
    try:
        # Try to get an IMDSv2 token.
        token = requests.put(
            url="http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": 100},
            timeout=timeout,
        ).text
    except:
        pass
    try:
        headers = {}
        # Add IMDSv2 token if available, else fall back to IMDSv1.
        if token:
            headers["X-aws-ec2-metadata-token"] = token
        instance_meta = requests.get(
            url="http://169.254.169.254/latest/dynamic/instance-identity/document",
            headers=headers,
            timeout=timeout,
        ).json()
        meta["ec2-instance-id"] = instance_meta.get("instanceId")
        meta["ec2-instance-type"] = instance_meta.get("instanceType")
        meta["ec2-region"] = instance_meta.get("region")
        meta["ec2-availability-zone"] = instance_meta.get("availabilityZone")
    except:
        pass
    return meta


def get_docker_registry(image_uri):
    """
    Explanation:
        (.+?(?:[:.].+?)\\/)? - [GROUP 0] REGISTRY
            .+?                  - A registry must start with at least one character
            (?:[:.].+?)\\/       - A registry must have ":" or "." and end with "/"
            ?                    - Make a registry optional
        (.*?)                - [GROUP 1] REPOSITORY
            .*?                  - Get repository name until separator
        (?:[@:])?            - SEPARATOR
            ?:                   - Don't capture separator
            [@:]                 - The separator must be either "@" or ":"
            ?                    - The separator is optional
        ((?<=[@:]).*)?       - [GROUP 2] TAG / DIGEST
            (?<=[@:])            - A tag / digest must be preceded by "@" or ":"
            .*                   - Capture rest of tag / digest
            ?                    - A tag / digest is optional
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
    supported_keys = set([*resource_defaults.keys(), *compute_deco.attributes.keys()])
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
                #
                # Skip attributes that are not supported by the decorator.
                if k not in supported_keys:
                    continue

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


def sanitize_batch_tag(key, value):
    """
    Sanitize a key and value for use as a Batch tag.
    """
    # https://docs.aws.amazon.com/batch/latest/userguide/using-tags.html#tag-restrictions
    RE_NOT_PERMITTED = r"[^A-Za-z0-9\s\+\-\=\.\_\:\/\@]"
    _key = re.sub(RE_NOT_PERMITTED, "", key)[:128]
    _value = re.sub(RE_NOT_PERMITTED, "", value)[:256]

    return _key, _value
