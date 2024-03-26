class AzureDefaultClientProvider(object):
    name = "azure-default"

    @staticmethod
    def create_cacheable_azure_credential(*args, **kwargs):
        """azure.identity.DefaultAzureCredential is not readily cacheable in a dictionary
        because it does not have a content based hash and equality implementations.

        We implement a subclass CacheableDefaultAzureCredential to add them.

        We need this because credentials will be part of the cache key in _ClientCache.
        """
        from azure.identity import DefaultAzureCredential

        class CacheableDefaultAzureCredential(DefaultAzureCredential):
            def __init__(self, *args, **kwargs):
                super(CacheableDefaultAzureCredential, self).__init__(*args, **kwargs)
                # Just hashing all the kwargs works because they are all individually
                # hashable as of 7/15/2022.
                #
                # What if Azure adds unhashable things to kwargs?
                # - We will have CI to catch this (it will always install the latest Azure SDKs)
                # - In Metaflow usage today we never specify any kwargs anyway. (see last line
                #   of the outer function.
                self._hash_code = hash((args, tuple(sorted(kwargs.items()))))

            def __hash__(self):
                return self._hash_code

            def __eq__(self, other):
                return hash(self) == hash(other)

        return CacheableDefaultAzureCredential(*args, **kwargs)


cached_provider_class = None


def create_cacheable_azure_credential():
    global cached_provider_class
    if cached_provider_class is None:
        from metaflow.metaflow_config import DEFAULT_AZURE_CLIENT_PROVIDER
        from metaflow.plugins import AZURE_CLIENT_PROVIDERS

        for p in AZURE_CLIENT_PROVIDERS:
            if p.name == DEFAULT_AZURE_CLIENT_PROVIDER:
                cached_provider_class = p
                break
        else:
            raise ValueError(
                "Cannot find Azure Client provider %s" % DEFAULT_AZURE_CLIENT_PROVIDER
            )
    return cached_provider_class.create_cacheable_azure_credential()
