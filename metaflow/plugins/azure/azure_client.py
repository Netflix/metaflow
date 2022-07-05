from metaflow.exception import MetaflowInternalError, MetaflowException
from metaflow.metaflow_config import AZURE_STORAGE_ACCOUNT_URL
from metaflow.plugins.azure.azure_python_version_check import check_python_version
from metaflow.plugins.azure.azure_utils import (
    CacheableDefaultAzureCredential,
    get_azure_storage_access_key,
)

check_python_version()

import os
import threading

try:

    # Python 3.6 would print lots of warnings about deprecated cryptography usage when importing Azure modules
    import warnings

    warnings.filterwarnings("ignore")
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
except ImportError:
    raise MetaflowInternalError(
        msg="Please ensure azure-identity and azure-storage-blob Python packages are installed"
    )


class _BlobServiceClientCache(object):
    """BlobServiceClient objects internally cache HTTPS connections. In order to reuse HTTPS connections,
    we need to reuse BlobServiceClient objects. The effect were are going for is for each process or
    thread in the executor to EACH REUSE ITS OWN long-lived BlobServiceClient object.

    This cache lives here at the module level because:

    * It must NOT live within AzureClient, because AzureClient must cross process boundaries.
      BlobServiceClient objects are not picklable. They also are not thread-safe, and so even if we
      were only using only ThreadPoolExecutor, the recommendation is to have one BlobServiceClient per
      thread.  See https://github.com/Azure/azure-storage-python/issues/559
    * It preferably should not live within AzureStorage, because AzureStorage is otherwise conceptually
      used ONLY within the main thread of the main process.
    """

    _cache = dict()

    def __init__(self):
        raise RuntimeError("BlobServiceClientCache may not be instantiated!")

    @staticmethod
    def get(
        storage_account_url,
        credential=None,
        max_single_put_size=None,
        max_single_get_size=None,
        max_chunk_get_size=None,
        connection_data_block_size=None,
    ):
        """
        Each spawned process will get its own fresh cache.

        With each process's cache, each thread gets its own distinct service object.

        The cache key includes all BlobServiceClient creation params PLUS process ID and thread ID.

        This means that no more than one thread of control will ever access a cache key. This, and
        the fact that dict() operations are thread-safe means that no additional synchronization
        required here.
        """
        cache_key = (
            os.getpid(),
            threading.get_ident(),
            credential,
            storage_account_url,
            max_single_put_size,
            max_single_get_size,
            max_chunk_get_size,
            connection_data_block_size,
        )
        service_just_created = None
        if cache_key not in _BlobServiceClientCache._cache:
            service_just_created = BlobServiceClient(
                storage_account_url,
                credential=credential,
                max_single_put_size=max_single_put_size,
                max_single_get_size=max_single_get_size,
                max_chunk_get_size=max_chunk_get_size,
                connection_data_block_size=connection_data_block_size,
                # BlobServiceClient accepts many more kwargs. We can add passthroughs as/when needed.
            )
            _BlobServiceClientCache._cache[cache_key] = service_just_created
        service_to_return = _BlobServiceClientCache._cache[cache_key]
        if (
            service_just_created is not None
            and service_just_created != service_to_return
        ):
            # This condition may not be fatal, but highly unexpected.
            # Each thread of execution gets its own entry, so there *should* be no concurrent insertions
            # on the same key.
            #
            # The Metaflow team REALLY wants to know if this ever happens, but we stop short of raising a
            # fatal exception to not kill user jobs.
            print(
                "METAFLOW WARNING: Azure BlobStorageServiceCache had the same cache key updated more than once"
            )
        return service_to_return


# Block size on underlying down TLS transport - Azure SDK defaults to 4096.
# This larger block size dramatically improves aggregate download throughput.
# TODO benchmark this... might pick a smaller number based on datastore bench
AZURE_CLIENT_CONNECTION_DATA_BLOCK_SIZE = 262144

# These controls when to use more than a single thread / connection for a single GET or PUT
AZURE_CLIENT_MAX_SINGLE_GET_SIZE_MB = 32
AZURE_CLIENT_MAX_SINGLE_PUT_SIZE_MB = 64

# Maximum chunk size when splitting a single blob GET into chunks for concurrent processing
AZURE_CLIENT_MAX_CHUNK_GET_SIZE_MB = 16

BYTES_IN_MB = 1024 * 1024


def get_azure_blob_service(
    storage_account_url=None,
    credential=None,
    credential_is_cacheable=False,  # TODO is hashable the best name?
    max_single_get_size=AZURE_CLIENT_MAX_SINGLE_GET_SIZE_MB * BYTES_IN_MB,
    max_single_put_size=AZURE_CLIENT_MAX_SINGLE_PUT_SIZE_MB * BYTES_IN_MB,
    max_chunk_get_size=AZURE_CLIENT_MAX_CHUNK_GET_SIZE_MB * BYTES_IN_MB,
    connection_data_block_size=AZURE_CLIENT_CONNECTION_DATA_BLOCK_SIZE * BYTES_IN_MB,
):
    if not storage_account_url:
        # TODO validate the URL to save user debug time (basic stuff)
        if not AZURE_STORAGE_ACCOUNT_URL:
            raise MetaflowException(
                msg="Must configure METAFLOW_AZURE_STORAGE_ACCOUNT_URL"
            )
        storage_account_url = AZURE_STORAGE_ACCOUNT_URL

    if not credential:
        access_key = get_azure_storage_access_key()
        if access_key:
            credential = access_key
            credential_is_cacheable = True
        else:
            credential = CacheableDefaultAzureCredential()
            credential_is_cacheable = True

    if not credential_is_cacheable:
        return BlobServiceClient(
            storage_account_url,
            credential=credential,
            max_single_put_size=max_single_put_size,
            max_single_get_size=max_single_get_size,
            max_chunk_get_size=max_chunk_get_size,
            connection_data_block_size=connection_data_block_size,
            # BlobServiceClient accepts many more kwargs. We can add passthroughs as/when needed.
        )

    return _BlobServiceClientCache.get(
        storage_account_url,
        credential=credential,
        max_single_put_size=max_single_put_size,
        max_single_get_size=max_single_get_size,
        max_chunk_get_size=max_chunk_get_size,
        connection_data_block_size=connection_data_block_size,
    )
