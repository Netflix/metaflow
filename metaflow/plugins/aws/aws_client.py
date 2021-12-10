cached_aws_sandbox_creds = None
cached_provider_class = None


class Boto3ClientProvider(object):
    name = "boto3"

    @staticmethod
    def get_client(module, with_error=False, params={}):
        from metaflow.exception import MetaflowException
        from metaflow.metaflow_config import (
            AWS_SANDBOX_ENABLED,
            AWS_SANDBOX_STS_ENDPOINT_URL,
            AWS_SANDBOX_API_KEY,
        )
        import requests

        try:
            import boto3
            from botocore.exceptions import ClientError
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'boto3'. Install boto3 first."
            )

        if AWS_SANDBOX_ENABLED:
            global cached_aws_sandbox_creds
            if cached_aws_sandbox_creds is None:
                # authenticate using STS
                url = "%s/auth/token" % AWS_SANDBOX_STS_ENDPOINT_URL
                headers = {"x-api-key": AWS_SANDBOX_API_KEY}
                try:
                    r = requests.get(url, headers=headers)
                    r.raise_for_status()
                    cached_aws_sandbox_creds = r.json()
                except requests.exceptions.HTTPError as e:
                    raise MetaflowException(repr(e))
            if with_error:
                return (
                    boto3.session.Session(**cached_aws_sandbox_creds).client(
                        module, **params
                    ),
                    ClientError,
                )
            return boto3.session.Session(**cached_aws_sandbox_creds).client(
                module, **params
            )
        if with_error:
            return boto3.client(module, **params), ClientError
        return boto3.client(module, **params)


def get_aws_client(module, with_error=False, params={}):
    global cached_provider_class
    if cached_provider_class is None:
        from metaflow.metaflow_config import DEFAULT_AWS_CLIENT_PROVIDER
        from metaflow.plugins import AWS_CLIENT_PROVIDERS

        for p in AWS_CLIENT_PROVIDERS:
            if p.name == DEFAULT_AWS_CLIENT_PROVIDER:
                cached_provider_class = p
                break
        else:
            raise ValueError(
                "Cannot find AWS Client provider %s" % DEFAULT_AWS_CLIENT_PROVIDER
            )
    return cached_provider_class.get_client(module, with_error, params)
