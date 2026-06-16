from metaflow.exception import MetaflowException
from metaflow.plugins.secrets.utils import get_default_secrets_backend_type


class SecretSpec:
    def __init__(self, secrets_backend_type, secret_id, options={}, role=None):
        self._secrets_backend_type = secrets_backend_type
        self._secret_id = secret_id
        self._options = options
        self._role = role

    @property
    def secrets_backend_type(self):
        return self._secrets_backend_type

    @property
    def secret_id(self):
        return self._secret_id

    @property
    def options(self):
        return self._options

    @property
    def role(self):
        return self._role

    def to_json(self):
        """Mainly used for testing... not the same as the input dict in secret_spec_from_dict()!"""
        return {
            "secrets_backend_type": self.secrets_backend_type,
            "secret_id": self.secret_id,
            "options": self.options,
            "role": self.role,
        }

    def __str__(self):
        return "%s (%s)" % (self._secret_id, self._secrets_backend_type)

    @staticmethod
    def secret_spec_from_str(secret_spec_str, role):
        # "." may be used in secret_id one day (provider specific). HOWEVER, it provides the best UX for
        # non-conflicting cases (i.e. for secret ids that don't contain "."). This is true for all AWS
        # Secrets Manager secrets.
        #
        # So we skew heavily optimize for best upfront UX for the present (1/2023).
        #
        # If/when a certain secret backend supports "." secret names, we can figure out a solution at that time.
        # At a minimum, dictionary style secret spec may be used with no code changes (see secret_spec_from_dict()).
        # Other options could be:
        #  - accept and document that "." secret_ids don't work in Metaflow (across all possible providers)
        #  - add a Metaflow config variable that specifies the separator (default ".")
        #  - smarter spec parsing, that errors on secrets that look ambiguous. "aws-secrets-manager.XYZ" could mean:
        #    + secret_id "XYZ" in aws-secrets-manager backend, OR
        #    + secret_id "aws-secrets-manager.XYZ" default backend (if it is defined).
        #    + in this case, user can simply set "azure-key-vault.aws-secrets-manager.XYZ" instead!
        parts = secret_spec_str.split(".", maxsplit=1)
        if len(parts) == 1:
            secrets_backend_type = get_default_secrets_backend_type()
            secret_id = parts[0]
        else:
            secrets_backend_type = parts[0]
            secret_id = parts[1]
        return SecretSpec(
            secrets_backend_type, secret_id=secret_id, options={}, role=role
        )

    @staticmethod
    def secret_spec_from_dict(secret_spec_dict, role):
        if "type" not in secret_spec_dict:
            secrets_backend_type = get_default_secrets_backend_type()
        else:
            secrets_backend_type = secret_spec_dict["type"]
            if not isinstance(secrets_backend_type, str):
                raise MetaflowException(
                    "Bad @secrets specification - 'type' must be a string - found %s"
                    % type(secrets_backend_type)
                )
        secret_id = secret_spec_dict.get("id")
        if not isinstance(secret_id, str):
            raise MetaflowException(
                "Bad @secrets specification - 'id' must be a string - found %s"
                % type(secret_id)
            )
        options = secret_spec_dict.get("options", {})
        if not isinstance(options, dict):
            raise MetaflowException(
                "Bad @secrets specification - 'option' must be a dict - found %s"
                % type(options)
            )
        role_for_source = secret_spec_dict.get("role", None)
        if role_for_source is not None:
            if not isinstance(role_for_source, str):
                raise MetaflowException(
                    "Bad @secrets specification - 'role' must be a str - found %s"
                    % type(role_for_source)
                )
            role = role_for_source
        return SecretSpec(
            secrets_backend_type, secret_id=secret_id, options=options, role=role
        )
