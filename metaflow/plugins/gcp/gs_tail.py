from io import BytesIO

from google.cloud.exceptions import NotFound, ClientError

from metaflow.exception import MetaflowException

from metaflow.plugins.gcp.gs_storage_client_factory import get_gs_storage_client
from metaflow.plugins.gcp.gs_utils import parse_gs_full_path


class GSTail(object):
    def __init__(self, blob_full_uri):
        """Location should be something like gs://<bucket_name>/blob"""
        bucket_name, blob_name = parse_gs_full_path(blob_full_uri)
        if not blob_name:
            raise MetaflowException(
                msg="Failed to parse blob_full_uri into gs://<bucket_name>/<blob_name> (got %s)"
                % blob_full_uri
            )
        client = get_gs_storage_client()
        bucket = client.bucket(bucket_name)
        self._blob_client = bucket.blob(blob_name)
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
            return self._blob_client.download_as_bytes(start=self._pos)
        except NotFound:
            return None
        except ClientError as e:
            # be silent on range errors - it means log did not advance
            if e.code != 416:
                print("Failed to tail log from step (status code = %d)" % (e.code,))
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

    parser = argparse.ArgumentParser(description="Tail an Google Cloud Storage blob.")
    parser.add_argument(
        "blob_full_uri", help="The blob to tail. Format is gs://<bucket_name>/<blob>"
    )
    args = parser.parse_args()
    gs_tail = GSTail(args.blob_full_uri)
    for line in gs_tail:
        print(line.strip().decode("utf-8"))
