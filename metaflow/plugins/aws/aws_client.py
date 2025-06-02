cached_aws_sandbox_creds = None
cached_provider_class = None


class Boto3ClientProvider(object):
    name = "boto3"

    @staticmethod
    def get_client(
        module, with_error=False, role_arn=None, session_vars=None, client_params=None
    ):
        from metaflow.exception import MetaflowException
        from metaflow.metaflow_config import (
            AWS_SANDBOX_ENABLED,
            AWS_SANDBOX_STS_ENDPOINT_URL,
            AWS_SANDBOX_API_KEY,
            S3_CLIENT_RETRY_CONFIG,
        )

        if session_vars is None:
            session_vars = {}

        if client_params is None:
            client_params = {}

        import requests

        try:
            import boto3
            import botocore
            from botocore.exceptions import ClientError
            from botocore.config import Config
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import module 'boto3'. Install boto3 first."
            )

        # Convert dictionary config to Config object if needed
        if "config" in client_params and not isinstance(
            client_params["config"], Config
        ):
            client_params["config"] = Config(**client_params["config"])

        if module == "s3" and (
            "config" not in client_params or client_params["config"].retries is None
        ):
            # do not set anything if the user has already set something
            config = client_params.get("config", Config())
            config.retries = S3_CLIENT_RETRY_CONFIG
            client_params["config"] = config

        if AWS_SANDBOX_ENABLED:
            # role is ignored in the sandbox
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
                        module, **client_params
                    ),
                    ClientError,
                )
            return boto3.session.Session(**cached_aws_sandbox_creds).client(
                module, **client_params
            )
        session = boto3.session.Session()
        if role_arn:
            fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
                client_creator=session._session.create_client,
                source_credentials=session._session.get_credentials(),
                role_arn=role_arn,
                extra_args={},
            )
            creds = botocore.credentials.DeferredRefreshableCredentials(
                method="assume-role", refresh_using=fetcher.fetch_credentials
            )
            botocore_session = botocore.session.Session(session_vars=session_vars)
            botocore_session._credentials = creds
            session = boto3.session.Session(botocore_session=botocore_session)
        if with_error:
            return session.client(module, **client_params), ClientError
        return session.client(module, **client_params)


def get_aws_client(
    module, with_error=False, role_arn=None, session_vars=None, client_params=None
):
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
    return cached_provider_class.get_client(
        module,
        with_error,
        role_arn=role_arn,
        session_vars=session_vars,
        client_params=client_params,
    )
