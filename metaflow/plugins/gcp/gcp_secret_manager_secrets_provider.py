import base64
import json
from json import JSONDecodeError


from metaflow.exception import MetaflowException
from metaflow.plugins.secrets import SecretsProvider
import re
from metaflow.plugins.gcp.gs_storage_client_factory import get_credentials
from metaflow.metaflow_config import GCP_SECRET_MANAGER_PREFIX


class MetaflowGcpSecretsManagerBadResponse(MetaflowException):
    """Raised when the response from GCP Secrets Manager is not valid in some way"""


class MetaflowGcpSecretsManagerDuplicateKey(MetaflowException):
    """Raised when the response from GCP Secrets Manager contains duplicate keys"""


class MetaflowGcpSecretsManagerJSONParseError(MetaflowException):
    """Raised when the SecretString response from GCP Secrets Manager is not valid JSON"""


class MetaflowGcpSecretsManagerNotJSONObject(MetaflowException):
    """Raised when the SecretString response from GCP Secrets Manager is not valid JSON dictionary"""


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


class GcpSecretManagerSecretsProvider(SecretsProvider):
    TYPE = "gcp-secret-manager"

    def get_secret_as_dict(self, secret_id, options={}, role=None):
        """
        Reads a secret from GCP Secrets Manager and returns it as a dictionary of environment variables.

        If the secret contains a string payload ("SecretString"):
        - if the `json` option is True:
            Secret will be parsed as a JSON. If successfully parsed, AND the JSON contains a
            top-level object, each entry K/V in the object will also be converted to an entry in the result. V will
            always be casted to a string (if not already a string).
        - If `json` option is False (default):
            Will be returned as a single entry in the result, with the key being the last part after / in secret_id.

        On GCP Secrets Manager, the secret payload is a binary blob. However, by default we interpret it as UTF8 encoded
        string. To disable this, set the `binary` option to True, the binary will be base64 encoded in the result.

        All keys in the result are sanitized to be more valid environment variable names. This is done on a best effort
        basis. Further validation is expected to be done by the invoking @secrets decorator itself.

        :param secret_id: GCP Secrets Manager secret ID
        :param options: unused
        :return: dict of environment variables. All keys and values are strings.
        """
        from google.cloud.secretmanager_v1.services.secret_manager_service import (
            SecretManagerServiceClient,
        )
        from google.cloud.secretmanager_v1.services.secret_manager_service.transports import (
            SecretManagerServiceTransport,
        )

        # Full secret id looks like projects/1234567890/secrets/mysecret/versions/latest
        #
        # We allow these forms of secret_id:
        #
        # 1. Full path like projects/1234567890/secrets/mysecret/versions/latest
        #    This is what you'd specify if you used to GCP SDK.
        #
        # 2. Full path but without the version like projects/1234567890/secrets/mysecret.
        #    This is what you see in the GCP console, makes it easier to copy & paste.
        #
        # 3. Simple string like mysecret
        #
        # 4. Simple string with /versions/<version> suffix like mysecret/versions/1

        # The latter two forms require METAFLOW_GCP_SECRET_MANAGER_PREFIX to be set.

        match_full = re.match(
            r"^projects/\d+/secrets/([\w\-]+)(/versions/([\w\-]+))?$", secret_id
        )
        match_partial = re.match(r"^([\w\-]+)(/versions/[\w\-]+)?$", secret_id)
        if match_full:
            # Full path
            env_var_name = match_full.group(1)
            if match_full.group(3):
                # With version specified
                full_secret_name = secret_id
            else:
                # No version specified, use latest
                full_secret_name = secret_id + "/versions/latest"
        elif match_partial:
            # Partial path, possibly with /versions/<version> suffix
            env_var_name = secret_id
            if not GCP_SECRET_MANAGER_PREFIX:
                raise ValueError(
                    "Cannot use simple secret_id without setting METAFLOW_GCP_SECRET_MANAGER_PREFIX. %s"
                    % GCP_SECRET_MANAGER_PREFIX
                )
            if match_partial.group(2):
                # With version specified
                full_secret_name = "%s%s" % (GCP_SECRET_MANAGER_PREFIX, secret_id)
                env_var_name = match_partial.group(1)
            else:
                # No version specified, use latest
                full_secret_name = "%s%s/versions/latest" % (
                    GCP_SECRET_MANAGER_PREFIX,
                    secret_id,
                )
        else:
            raise ValueError(
                "Invalid secret_id: %s. Must be either a full path or a simple string."
                % secret_id
            )

        result = {}

        def _sanitize_and_add_entry_to_result(k, v):
            # Two jobs - sanitize, and check for dupes
            sanitized_k = _sanitize_key_as_env_var(k)
            if sanitized_k in result:
                raise MetaflowGcpSecretsManagerDuplicateKey(
                    "Duplicate key in secret: '%s' (sanitizes to '%s')"
                    % (k, sanitized_k)
                )
            result[sanitized_k] = v

        credentials, _ = get_credentials(
            scopes=SecretManagerServiceTransport.AUTH_SCOPES
        )
        client = SecretManagerServiceClient(credentials=credentials)
        response = client.access_secret_version(request={"name": full_secret_name})
        payload_str = response.payload.data.decode("UTF-8")
        if options.get("json", False):
            obj = json.loads(payload_str)
            if type(obj) == dict:
                for k, v in obj.items():
                    # We try to make it work here - cast to string always
                    _sanitize_and_add_entry_to_result(k, str(v))
            else:
                raise MetaflowGcpSecretsManagerNotJSONObject(
                    "Secret string is a JSON, but not an object (dict-like) - actual type %s."
                    % type(obj)
                )
        else:
            if options.get("env_var_name"):
                env_var_name = options["env_var_name"]

            if options.get("binary", False):
                _sanitize_and_add_entry_to_result(
                    env_var_name, base64.b64encode(response.payload.data)
                )
            else:
                _sanitize_and_add_entry_to_result(env_var_name, payload_str)

        return result
