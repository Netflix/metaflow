
export METAFLOW_DATASTORE_SYSROOT_AZURE=test-azure-storage/hotdog-core/$(date +%s)
export METAFLOW_AZURE_STORAGE_ACCOUNT_URL=https://obbenchmark1.blob.core.windows.net/
export METAFLOW_ARTIFACT_LOCALROOT=/mnt/metaflow-tmp
export METAFLOW_CLIENT_CACHE_PATH=/mnt/metaflow_client
export METAFLOW_DATASTORE_SYSROOT_LOCAL=/mnt/.metaflow

# export METAFLOW_AZURE_STORAGE_ACCESS_KEY=

rm -rf $METAFLOW_ARTIFACT_LOCALROOT $METAFLOW_CLIENT_CACHE_PATH $METAFLOW_DATASTORE_SYSROOT_LOCAL
mkdir -p $METAFLOW_ARTIFACT_LOCALROOT

PYTHONPATH=$(pwd)/../.. python run_tests.py --inherit-env --num-parallel 48 --contexts python3-all-local-azure-storage
