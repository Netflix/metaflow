import sys

from metaflow.exception import MetaflowException

# The backend uses APIs from the `tenki` SDK >= 0.5.4 (Client(auth_token=),
# create, CommandResult fields). 0.5.4 requires Python >= 3.10.
_MIN_TENKI_VERSION = "0.5.4"


class TenkiException(MetaflowException):
    headline = "Tenki error"


class TenkiKilledException(MetaflowException):
    headline = "Tenki sandbox killed"


def get_tenki_module():
    # The `tenki` SDK is a soft dependency, imported lazily so that Metaflow can
    # be installed and used without it (mirrors how the Kubernetes and AWS
    # plugins treat their respective SDKs).
    try:
        import tenki
    except (NameError, ImportError):
        raise TenkiException(
            "Could not import module 'tenki'.\n\n"
            "Install the Tenki SDK first (>= %s, needs Python >= 3.10):\n"
            "    %s -m pip install 'tenki>=%s'\n"
            "or equivalent through your favorite Python package manager."
            % (_MIN_TENKI_VERSION, sys.executable, _MIN_TENKI_VERSION)
        )

    _check_min_version()
    return tenki


def _check_min_version():
    # The SDK does not expose __version__, so read the installed distribution
    # metadata. Enforce the minimum the backend was written against; degrade
    # gracefully (do not block) if the version cannot be determined or parsed.
    try:
        from importlib.metadata import version, PackageNotFoundError
    except ImportError:
        return
    try:
        installed = version("tenki")
    except PackageNotFoundError:
        return
    try:
        from metaflow._vendor.packaging.version import Version, InvalidVersion
    except ImportError:
        return
    try:
        too_old = Version(installed) < Version(_MIN_TENKI_VERSION)
    except InvalidVersion:
        return
    if too_old:
        raise TenkiException(
            "The installed tenki %s is too old; @tenki requires >= %s (which "
            "needs Python >= 3.10). Upgrade with:\n"
            "    %s -m pip install -U 'tenki>=%s'"
            % (
                installed,
                _MIN_TENKI_VERSION,
                sys.executable,
                _MIN_TENKI_VERSION,
            )
        )


class TenkiClient(object):
    """Thin wrapper around the ``tenki`` SDK.

    Keeps the SDK a soft dependency (imported lazily) and centralizes the auth
    configuration so the runner does not have to know how the SDK is
    constructed. All contact with the SDK surface lives here and in
    ``tenki.py`` so that adjusting to the exact SDK signature is a one-file
    change.
    """

    def __init__(self, api_key=None, base_url=None):
        self._sdk = get_tenki_module()
        # Auth is configured on the Client. When auth_token is not passed, the
        # SDK resolves it from the environment (TENKI_AUTH_TOKEN / TENKI_API_KEY).
        client_kwargs = {}
        if api_key:
            client_kwargs["auth_token"] = api_key
        if base_url:
            client_kwargs["base_url"] = base_url
        self._client = self._sdk.Client(**client_kwargs)

    def create_sandbox(self, **kwargs):
        return self._client.create(**kwargs)

    def list_sandboxes(self, tags=None):
        return self._client.list(tags=tags) if tags else self._client.list()

    def exception(self, name):
        # Return an SDK exception class by name, or an empty tuple if the SDK
        # does not expose it (so `isinstance(err, self.exception("X"))` is
        # always safe).
        return getattr(self._sdk, name, ())

    def close(self):
        close = getattr(self._client, "close", None)
        if close is not None:
            try:
                close()
            except Exception:
                pass
