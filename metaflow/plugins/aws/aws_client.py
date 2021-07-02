cached_aws_sandbox_creds = None


def get_aws_client(module, with_error=False, params={}):
    from metaflow.exception import MetaflowException
    from metaflow.metaflow_config import AWS_SANDBOX_ENABLED, \
        AWS_SANDBOX_STS_ENDPOINT_URL, AWS_SANDBOX_API_KEY
    import requests
    try:
        import boto3
        from botocore.exceptions import ClientError
    except (NameError, ImportError):
        raise MetaflowException(
            "Could not import module 'boto3'. Install boto3 first.")

    if AWS_SANDBOX_ENABLED:
        global cached_aws_sandbox_creds
        if cached_aws_sandbox_creds is None:
            # authenticate using STS
            url = "%s/auth/token" % AWS_SANDBOX_STS_ENDPOINT_URL
            headers = {
                'x-api-key': AWS_SANDBOX_API_KEY
            }
            try:
                r = requests.get(url, headers=headers)
                r.raise_for_status()
                cached_aws_sandbox_creds = r.json()
            except requests.exceptions.HTTPError as e:
                raise MetaflowException(repr(e))
        if with_error:
            return boto3.session.Session(
                **cached_aws_sandbox_creds).client(module, **params), ClientError
        return boto3.session.Session(
            **cached_aws_sandbox_creds).client(module, **params)
    if with_error:
        return boto3.client(module, **params), ClientError
    return boto3.client(module, **params)
