from io import BytesIO
from .s3util import aws_retry, get_s3_client

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse


class S3Tail(object):
    def __init__(self, s3url):
        url = urlparse(s3url)
        self.s3, self.ClientError = get_s3_client()
        self._bucket = url.netloc
        self._key = url.path.lstrip("/")
        self._pos = 0
        self._tail = b""

    def reset_client(self, hard_reset=False):
        # This method is required by @aws_retry
        if hard_reset or self.s3 is None:
            self.s3, self.ClientError = get_s3_client()

    def clone(self, s3url):
        tail = S3Tail(s3url)
        tail._pos = self._pos
        tail._tail = self._tail
        return tail

    @property
    def bytes_read(self):
        return self._pos

    @property
    def tail(self):
        return self._tail

    def __iter__(self):
        buf = self._fill_buf()
        if buf is not None:
            for line in buf:
                if line.endswith(b"\n"):
                    yield line
                else:
                    self._tail = line
                    break

    @aws_retry
    def _make_range_request(self):
        try:
            return self.s3.get_object(
                Bucket=self._bucket, Key=self._key, Range="bytes=%d-" % self._pos
            )
        except self.ClientError as err:
            code = err.response["Error"]["Code"]
            # NOTE we deliberately regard NoSuchKey as an ignorable error.
            # We assume that the file just hasn't appeared in S3 yet.
            # Some S3 compatible storage systems like Dell EMC-ECS return 416 in-lieu
            # of InvalidRange - https://www.delltechnologies.com/asset/en-us/products/storage/technical-support/docu95766.pdf
            if code in ("InvalidRange", "NoSuchKey", "416"):
                return None
            else:
                raise

    def _fill_buf(self):
        resp = self._make_range_request()
        if resp is None:
            return None
        code = str(resp["ResponseMetadata"]["HTTPStatusCode"])
        if code[0] == "2":
            data = resp["Body"].read()
            if data:
                buf = BytesIO(self._tail + data)
                self._pos += len(data)
                self._tail = b""
                return buf
            else:
                return None
        elif code[0] == "5":
            return None
        else:
            raise Exception(
                "Retrieving %s/%s failed: %s" % (self._bucket, self._key, code)
            )
