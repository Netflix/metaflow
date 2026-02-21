import re

from metaflow.exception import MetaflowException


def get_credential_debug_info(s3config=None):
    """
    Return a human-readable string describing the active AWS credentials.

    When s3config.role is set, mirrors the AssumeRole credential chain used by
    aws_client.py so the reported identity matches what actually made the S3 call.
    When no role is configured, reflects ambient boto3 credential resolution.

    Never raises — failures are reported inline.
    """
    lines = []

    try:
        # Lazy imports: boto3/botocore are optional dependencies and may not be
        # present in all Metaflow deployments. Importing here avoids import-time
        # errors in environments that do not use AWS at all.
        import boto3
        import botocore.config
        import botocore.credentials
        import botocore.session as botocore_session_module

        # Extract the configured IAM role ARN, if any. getattr guards against
        # s3config namedtuple fields being absent in older versions.
        role = s3config and getattr(s3config, "role", None)

        # Always start from a plain ambient session. When a role is configured
        # this becomes the *source* session for AssumeRole; otherwise it is used
        # directly for credential inspection and the STS call.
        base_session = boto3.session.Session()

        if role:
            # Replicate the exact credential chain from aws_client.py so the
            # STS get_caller_identity() call reflects the identity that actually
            # performed the failing S3 operation, not the base caller.
            #
            # AssumeRoleCredentialFetcher lazily calls sts:AssumeRole on first
            # use. DeferredRefreshableCredentials wraps it so that credentials
            # are auto-refreshed before they expire — matching the behaviour of
            # the live S3 client that triggered the 403.
            fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
                client_creator=base_session._session.create_client,
                source_credentials=base_session._session.get_credentials(),
                role_arn=role,
                extra_args={},
            )
            creds = botocore.credentials.DeferredRefreshableCredentials(
                method="assume-role", refresh_using=fetcher.fetch_credentials
            )
            # Inject the assumed-role credentials into a fresh botocore session,
            # then wrap it as a boto3 Session so we can call .client() normally.
            bc_session = botocore_session_module.Session()
            bc_session._credentials = creds
            session = boto3.session.Session(botocore_session=bc_session)
            lines.append("  Credential source : AssumeRole")
            lines.append("  Role ARN          : %s" % role)
        else:
            # No role configured — inspect ambient credentials directly.
            session = base_session
            credentials = session.get_credentials()

            if credentials is None:
                lines.append("  No AWS credentials found in the credential chain.")
            else:
                # `method` identifies which provider in the credential chain
                # resolved the credentials (e.g. "env", "iam-role").
                method = getattr(credentials, "method", None) or "unknown"

                # get_frozen_credentials() resolves any lazy/refreshable wrapper
                # and returns a plain FrozenCredentials with stable field access.
                if hasattr(credentials, "get_frozen_credentials"):
                    frozen = credentials.get_frozen_credentials()
                else:
                    frozen = credentials

                # Map botocore's internal provider method names to human-readable
                # labels for display. Falls back to the raw method string for any
                # provider not listed here (e.g. a custom credential plugin).
                method_labels = {
                    "env": "Environment variables",
                    "shared-credentials-file": "Shared credentials file (~/.aws/credentials)",
                    "config-file": "AWS config file (~/.aws/config)",
                    "iam-role": "EC2 instance IAM role (IMDS)",
                    "container-role": "ECS / EKS container IAM role",
                    "web-identity": "Web identity token (OIDC / IRSA)",
                    "assume-role": "AssumeRole",
                    "process": "Credential process",
                }

                lines.append(
                    "  Credential source : %s" % method_labels.get(method, method)
                )

                # Mask the access key so it is identifiable but not leakable in
                # logs. Only the first and last 4 characters are shown.
                access_key = getattr(frozen, "access_key", None) or ""
                if len(access_key) >= 8:
                    masked = "%s...%s" % (access_key[:4], access_key[-4:])
                elif access_key:
                    masked = "(too short to display safely)"
                else:
                    masked = "(not available)"

                lines.append("  Access key ID     : %s" % masked)

        region = session.region_name or "(not set)"
        lines.append("  AWS region        : %s" % region)

        # STS get_caller_identity() is the authoritative source of the effective
        # identity: it reflects what AWS sees as the caller, including any assumed
        # role chain. The call uses the same session constructed above (base or
        # assumed-role) so the reported identity matches the failing S3 client.
        # A short timeout prevents this debug call from blocking noticeably.
        try:
            config = botocore.config.Config(connect_timeout=5, read_timeout=5)
            sts = session.client("sts", config=config)
            identity = sts.get_caller_identity()

            lines.append("  Effective identity (STS):")
            lines.append("    ARN        : %s" % identity.get("Arn", "(unknown)"))
            lines.append("    Account ID : %s" % identity.get("Account", "(unknown)"))
            lines.append("    User ID    : %s" % identity.get("UserId", "(unknown)"))

        except Exception as sts_err:
            # Non-fatal: the STS call itself might be denied or unreachable.
            # Surface the error inline rather than suppressing it entirely so the
            # caller still gets the partial credential info gathered above.
            lines.append("  Effective identity (STS lookup failed): %s" % sts_err)

    except Exception as e:
        # Catch-all so that a bug in this debug helper never propagates to the
        # caller. The original access-denied error is what matters; this info
        # is supplementary.
        lines.append("  (Could not retrieve credential info: %s)" % e)

    # The header and tip bracket the per-field lines collected above.
    header = "Credential debug info (METAFLOW_DEBUG_S3CLIENT=1):"
    tip = (
        "Tip: Verify this identity has required S3 permissions "
        "(s3:GetObject, s3:PutObject, s3:ListBucket) on the target bucket."
    )
    return "\n".join([header] + lines + ["", tip])



def parse_s3_full_path(s3_uri):
    from urllib.parse import urlparse

    #  <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
    scheme, netloc, path, _, _, _ = urlparse(s3_uri)
    assert scheme == "s3"
    assert netloc is not None

    bucket = netloc
    path = path.lstrip("/").rstrip("/")
    if path == "":
        path = None

    return bucket, path


def get_ec2_instance_metadata():
    """
    Fetches the EC2 instance metadata through AWS instance metadata service

    Returns either an empty dictionary, or one with the keys
        - ec2-instance-id
        - ec2-instance-type
        - ec2-region
        - ec2-availability-zone
    """

    # TODO: Remove dependency on requests
    import requests

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
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "100"},
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
                            # TODO: Throw a better exception since the user has no
                            #       knowledge of 'compute' decorator
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


def validate_aws_tag(key: str, value: str):
    PERMITTED = r"[A-Za-z0-9\s\+\-\=\.\_\:\/\@]"

    AWS_PREFIX = r"^aws\:"  # case-insensitive.
    if re.match(AWS_PREFIX, key, re.IGNORECASE) or re.match(
        AWS_PREFIX, value, re.IGNORECASE
    ):
        raise MetaflowException(
            "'aws:' is not an allowed prefix for either tag keys or values"
        )

    if len(key) > 128:
        raise MetaflowException(
            "Tag key *%s* is too long. Maximum allowed tag key length is 128." % key
        )
    if len(value) > 256:
        raise MetaflowException(
            "Tag value *%s* is too long. Maximum allowed tag value length is 256."
            % value
        )

    if not re.match(PERMITTED, key):
        raise MetaflowException(
            "Key *s* is not permitted. Tags must match pattern: %s" % (key, PERMITTED)
        )
    if not re.match(PERMITTED, value):
        raise MetaflowException(
            "Value *%s* is not permitted. Tags must match pattern: %s"
            % (value, PERMITTED)
        )
        
