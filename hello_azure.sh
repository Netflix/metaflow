set -x

export METAFLOW_DATASTORE_SYSROOT_AZURE=test-azure-storage/hello-azure-demo
export METAFLOW_AZURE_STORAGE_ACCOUNT_URL=https://obbenchmark1.blob.core.windows.net/

python helloworld.py --datastore=azure run


