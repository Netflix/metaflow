from metaflow.plugins.azure.azure_python_version_check import check_python_version

check_python_version()

from metaflow.datastore.azure_exceptions import (
    MetaflowAzureAuthenticationError,
    MetaflowAzureResourceError,
)
from metaflow.exception import MetaflowInternalError


try:

    # Python 3.6 would print lots of warnings about deprecated cryptography usage when importing Azure modules
    import warnings

    warnings.filterwarnings("ignore")
    from azure.core.exceptions import (
        ClientAuthenticationError,
        ResourceNotFoundError,
        ResourceExistsError,
        AzureError,
    )

except ImportError:
    raise MetaflowInternalError(
        msg="Please ensure azure-identity and azure-storage-blob Python packages are installed"
    )


def parse_azure_sysroot(sysroot):
    """
    Parse a sysroot path str into a tuple (container_name, blob_prefix).

    - container_name is the Azure Blob Storage container name
    - blob_prefix is subpath within that container to which blobs are to live

    We take a strict validation approach, doing no implicit string manipulations on
    the user's behalf.  Path manipulations by themselves are complicated enough without
    adding magic.

    We provide clear error messages so the user knows exactly how to fix any validation error.
    """
    if sysroot.endswith("/"):
        raise ValueError("sysroot may not end with slash (got %s)" % sysroot)
    if sysroot.startswith("/"):
        raise ValueError("sysroot may not start with slash (got %s)" % sysroot)
    if "//" in sysroot:
        raise ValueError(
            "sysroot may not contain any consecutive slashes (got %s)" % sysroot
        )
    parts = sysroot.split("/", 1)
    container_name = parts[0]
    if container_name == "":
        raise ValueError(
            "Container name part of sysroot may not be empty (tried to parse %s)"
            % (sysroot,)
        )
    if len(parts) == 1:
        blob_prefix = None
    else:
        blob_prefix = parts[1]

    return container_name, blob_prefix


def process_exception(e):
    """
    Translate errors to Metaflow errors for standardized messaging. The intent is that all
    Azure Blob Storage integration logic should send any errors to this function for
    translation.

    We explicitly EXCLUDE executor related errors here.  See handle_executor_exceptions
    """
    if isinstance(e, ClientAuthenticationError):
        raise MetaflowAzureAuthenticationError(msg=str(e).splitlines()[-1])
    elif isinstance(e, (ResourceNotFoundError, ResourceExistsError)):
        raise MetaflowAzureResourceError(msg=str(e))
    elif isinstance(e, AzureError):  # this is the base class for all Azure SDK errors
        raise MetaflowInternalError(msg="Azure error: %s" % (str(e)))
    else:
        raise MetaflowInternalError(msg=str(e))


def handle_exceptions(func):
    """This is a decorator leveraging the logic from process_exception()"""

    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            process_exception(e)

    return inner_function
