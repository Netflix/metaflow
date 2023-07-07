import base64
import json
from json import JSONDecodeError


from metaflow.exception import MetaflowException
from metaflow.metaflow_config import AWS_SECRETS_MANAGER_DEFAULT_REGION
from metaflow.plugins.secrets import SecretsProvider
import re


class MetaflowAWSSecretsManagerBadResponse(MetaflowException):
    """Raised when the response from AWS Secrets Manager is not valid in some way"""


class MetaflowAWSSecretsManagerDuplicateKey(MetaflowException):
    """Raised when the response from AWS Secrets Manager contains duplicate keys"""


class MetaflowAWSSecretsManagerJSONParseError(MetaflowException):
    """Raised when the SecretString response from AWS Secrets Manager is not valid JSON"""


class MetaflowAWSSecretsManagerNotJSONObject(MetaflowException):
    """Raised when the SecretString response from AWS Secrets Manager is not valid JSON object (dictionary)"""


def _sanitize_key_as_env_var(key):
    """
    Sanitize a key as an environment variable name.
    This is purely a convenience trade-off to cover common cases well, vs. introducing
    ambiguities (e.g. did the final '_' come from '.', or '-' or is original?).

    1/27/2023(jackie):

    We start with few rules and should *sparingly* add more over time.
    Also, it's TBD whether all possible providers will share the same sanitization logic.
    Therefore we will keep this function private for now
    """
    return key.replace("-", "_").replace(".", "_").replace("/", "_")


class AwsSecretsManagerSecretsProvider(SecretsProvider):
    TYPE = "aws-secrets-manager"

    def get_secret_as_dict(self, secret_id, options={}, role=None):
        """
        Reads a secret from AWS Secrets Manager and returns it as a dictionary of environment variables.

        The secret payload from AWS is EITHER a string OR a binary blob.

        If the secret contains a string payload ("SecretString"):
        - if the `parse_secret_string_as_json` option is True (default):
            {SecretString} will be parsed as a JSON. If successfully parsed, AND the JSON contains a
            top-level object, each entry K/V in the object will also be converted to an entry in the result. V will
            always be casted to a string (if not already a string).
        - If `parse_secret_string_as_json` option is False:
            {SecretString} will be returned as a single entry in the result, with the key being the secret_id.

        Otherwise, the secret contains a binary blob payload ("SecretBinary"). In this case
        - The result dic contains '{SecretName}': '{SecretBinary}', where {SecretBinary} is a base64-encoded string

        All keys in the result are sanitized to be more valid environment variable names. This is done on a best effort
        basis. Further validation is expected to be done by the invoking @secrets decorator itself.

        :param secret_id: ARN or friendly name of the secret
        :param options: unused
        :param role: AWS IAM Role ARN to assume before reading the secret
        :return: dict of environment variables. All keys and values are strings.
        """
        import botocore
        from metaflow.plugins.aws.aws_client import get_aws_client

        effective_aws_region = None
        # arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters
        m = re.match("arn:aws:secretsmanager:([^:]+):", secret_id)
        if m:
            effective_aws_region = m.group(1)
        elif "region" in options:
            effective_aws_region = options["region"]
        else:
            effective_aws_region = AWS_SECRETS_MANAGER_DEFAULT_REGION

        # At the end of all that, `effective_aws_region` may still be None.
        # This might still be OK, if there is fallback AWS region info in environment like:
        # .aws/config or AWS_REGION env var or AWS_DEFAULT_REGION env var, etc.
        try:
            secrets_manager_client = get_aws_client(
                "secretsmanager",
                client_params={"region_name": effective_aws_region},
                role_arn=role,
            )
        except botocore.exceptions.NoRegionError:
            # We try our best with a nice error message.
            # When run in Kubernetes or Argo Workflows, the traceback is still monstrous.
            # TODO: Find a way to show a concise error in logs
            raise MetaflowException(
                "Default region is not specified for AWS Secrets Manager. Please set METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION"
            )
        result = {}

        def _sanitize_and_add_entry_to_result(k, v):
            # Two jobs - sanitize, and check for dupes
            sanitized_k = _sanitize_key_as_env_var(k)
            if sanitized_k in result:
                raise MetaflowAWSSecretsManagerDuplicateKey(
                    "Duplicate key in secret: '%s' (sanitizes to '%s')"
                    % (k, sanitized_k)
                )
            result[sanitized_k] = v

        """
        These are the exceptions that can be raised by the AWS SDK:
        
        SecretsManager.Client.exceptions.ResourceNotFoundException
        SecretsManager.Client.exceptions.InvalidParameterException
        SecretsManager.Client.exceptions.InvalidRequestException
        SecretsManager.Client.exceptions.DecryptionFailure
        SecretsManager.Client.exceptions.InternalServiceError
        
        Looks pretty informative already, so we won't catch here directly.
        
        1/27/2023(jackie) - We will evolve this over time as we learn more.
        """
        response = secrets_manager_client.get_secret_value(SecretId=secret_id)
        if "Name" not in response:
            raise MetaflowAWSSecretsManagerBadResponse(
                "Secret 'Name' is missing in response"
            )
        secret_name = response["Name"]
        if "SecretString" in response:
            secret_str = response["SecretString"]
            if options.get("json", True):
                try:
                    obj = json.loads(secret_str)
                    if type(obj) == dict:
                        for k, v in obj.items():
                            # We try to make it work here - cast to string always
                            _sanitize_and_add_entry_to_result(k, str(v))
                    else:
                        raise MetaflowAWSSecretsManagerNotJSONObject(
                            "Secret string is a JSON, but not an object (dict-like) - actual type %s."
                            % type(obj)
                        )
                except JSONDecodeError:
                    raise MetaflowAWSSecretsManagerJSONParseError(
                        "Secret string could not be parsed as JSON"
                    )
            else:
                if options.get("env_var_name"):
                    env_var_name = options["env_var_name"]
                else:
                    env_var_name = secret_name
                _sanitize_and_add_entry_to_result(env_var_name, secret_str)

        elif "SecretBinary" in response:
            # boto3 docs say response gives base64 encoded, but it's wrong.
            # See https://github.com/boto/boto3/issues/2735
            # In reality, we get raw bytes.  We will encode it ourselves to become env var ready.
            # Note env vars values may not contain null bytes.... therefore we cannot leave it as
            # bytes.
            #
            # The trailing decode gives us a final UTF-8 string.
            if options.get("env_var_name"):
                env_var_name = options["env_var_name"]
            else:
                env_var_name = secret_name
            _sanitize_and_add_entry_to_result(
                env_var_name, base64.b64encode(response["SecretBinary"]).decode()
            )
        else:
            raise MetaflowAWSSecretsManagerBadResponse(
                "Secret response is missing both 'SecretString' and 'SecretBinary'"
            )
        return result
