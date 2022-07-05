set -x

export METAFLOW_DATASTORE_SYSROOT_AZURE=test-azure-storage/hello-azure-demo
export METAFLOW_AZURE_STORAGE_ACCOUNT_URL=https://obbenchmark1.blob.core.windows.net/
# export METAFLOW_AZURE_STORAGE_ACCESS_KEY=

METAFLOW_AZURE_STORAGE_WORKLOAD_TYPE=general python helloworld.py --datastore=azure run
METAFLOW_AZURE_STORAGE_WORKLOAD_TYPE=high_throughput python helloworld.py --datastore=azure run


