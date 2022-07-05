import sys
from io import BytesIO

from azure.core.exceptions import ResourceNotFoundError

from metaflow.exception import MetaflowException
from metaflow.plugins.azure.azure_python_version_check import check_python_version

check_python_version()

from metaflow.plugins.azure.azure_client import get_azure_blob_service
from metaflow.plugins.azure.azure_utils import parse_azure_sysroot


class AzureTail(object):
    def __init__(self, blob_full_uri):
        """Location should be something like <container_name>/blob"""
        container_name, blob_prefix = parse_azure_sysroot(blob_full_uri)
        # TODO naming is awkward here
        blob = blob_prefix
        if not blob:
            raise MetaflowException(
                msg="Failed to parse blob_full_uri into <container_name>/<blob_name>"
            )
        service = get_azure_blob_service()
        container = service.get_container_client(container_name)
        self._blob_client = container.get_blob_client(blob)
        self._pos = 0
        self._tail = b""

    def __iter__(self):
        buf = self._fill_buf()
        if buf is not None:
            # If there are no line breaks in the entries
            # file, then we will yield nothing, ever.
            #
            # This apes S3 tail. We can fix it here and in S3
            # if/when this becomes an issue. It boils down to
            # knowing when to give up waiting on partial lines
            # to become full lines (tricky, need more info).
            #
            # Likely this has been OK because we control the
            # line-break presence in the objects we tail.
            for line in buf:
                if line.endswith(b"\n"):
                    yield line
                else:
                    self._tail = line
                    break

    def _make_range_request(self):
        try:
            # Yes we read to the end... memory blow up is possible. We can improve by specifying length param
            return self._blob_client.download_blob(offset=self._pos).readall()
        except ResourceNotFoundError:
            # Maybe the log hasn't been uploaded yet, but will be soon.
            return None

    def _fill_buf(self):
        data = self._make_range_request()
        if data is None:
            return None
        if data:
            buf = BytesIO(data)
            self._pos += len(data)
            self._tail = b""
            return buf
        else:
            return None


if __name__ == "__main__":
    # This main program is provided for debugging and testing purposes only
    import argparse

    parser = argparse.ArgumentParser(
        description="Tail an Azure Blob. Must specify METAFLOW_AZURE_STORAGE_ACCOUNT_URL in environment."
    )
    parser.add_argument(
        "blob_full_uri", help="The blob to tail. Format is <container_name>/<blob>"
    )
    args = parser.parse_args()
    az_tail = AzureTail(args.blob_full_uri)
    for line in az_tail:
        print(line.strip().decode("utf-8"))
