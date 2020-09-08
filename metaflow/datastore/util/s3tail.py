
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
        self._key = url.path.lstrip('/')
        self._pos = 0
        self._tail = b''

    @property
    def bytes_read(self):
        return self._pos

    @property
    def tail(self):
        return self._tail

    def __iter__(self):
        while True:
            buf = self._fill_buf()
            if buf is None:
                yield b''
            else:
                for line in buf:
                    if line.endswith(b'\n'):
                        yield line
                    else:
                        self._tail = line
                        break

    @aws_retry
    def _make_range_request(self):
        try:
            return self.s3.get_object(Bucket=self._bucket,
                                      Key=self._key,
                                      Range='bytes=%d-' % self._pos)
        except self.ClientError as err:
            code = err.response['Error']['Code']
            # NOTE we deliberately regard NoSuchKey as an ignorable error.
            # We assume that the file just hasn't appeared in S3 yet.
            if code in ('InvalidRange', 'NoSuchKey'):
                return None
            else:
                raise

    def _fill_buf(self):
        resp = self._make_range_request()
        if resp is None:
            return None
        code = str(resp['ResponseMetadata']['HTTPStatusCode'])
        if code[0] == '2':
            data = resp['Body'].read()
            if data:
                buf = BytesIO(self._tail + data)
                self._pos += len(data)
                self._tail = b''
                return buf
            else:
                return None
        elif code[0] == '5':
            return None
        else:
            raise Exception('Retrieving %s/%s failed: %s' % (self.bucket, self.key, code))

