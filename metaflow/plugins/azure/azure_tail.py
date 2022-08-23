from io import BytesIO

from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from metaflow.exception import MetaflowException

from metaflow.plugins.azure.blob_service_client_factory import (
    get_azure_blob_service_client,
)
from metaflow.plugins.azure.azure_utils import (
    parse_azure_full_path,
)


class AzureTail(object):
    def __init__(self, blob_full_uri):
        """Location should be something like <container_name>/blob"""
        container_name, blob_name = parse_azure_full_path(blob_full_uri)
        if not blob_name:
            raise MetaflowException(
                msg="Failed to parse blob_full_uri into <container_name>/<blob_name> (got %s)"
                % blob_full_uri
            )
        service = get_azure_blob_service_client()
        container = service.get_container_client(container_name)
        self._blob_client = container.get_blob_client(blob_name)
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
        except HttpResponseError as e:
            # be silent on range errors - it means log did not advance
            if e.status_code != 416:
                print(
                    "Failed to tail log from step (status code = %d)" % (e.status_code,)
                )
            return None
        except Exception as e:
            print("Failed to tail log from step (%s)" % type(e))
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
    # This main program is for debugging and testing purposes
    import argparse

    parser = argparse.ArgumentParser(
        description="Tail an Azure Blob. Must specify METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT in environment."
    )
    parser.add_argument(
        "blob_full_uri", help="The blob to tail. Format is <container_name>/<blob>"
    )
    args = parser.parse_args()
    az_tail = AzureTail(args.blob_full_uri)
    for line in az_tail:
        print(line.strip().decode("utf-8"))
