from __future__ import print_function
import random
import time
import sys

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DEFAULT_AUTH, S3_ENDPOINT_URL, S3_VERIFY_CERTIFICATE
from botocore.exceptions import ClientError

S3_NUM_RETRIES = 7

def get_s3_client():
    from metaflow.plugins import AUTH_PROVIDERS
    try:
        import boto3
        from botocore.exceptions import ClientError
    except (NameError, ImportError):
        raise MetaflowException("Could not import module 'boto3' which "
                                "is required by S3 datastore. Install boto "
                                "first.")
    get_client = AUTH_PROVIDERS[DEFAULT_AUTH]
    # If DEFAULT_AUTH does not support s3, this should error. We do not check for
    # specific values of DEFAULT_AUTH as multiple auth methods may be backed by S3
    # particularly for custom enterprise systems.
    return get_client(
        's3',
        { 'endpoint_url': S3_ENDPOINT_URL, 'verify': S3_VERIFY_CERTIFICATE }), ClientError

# decorator to retry functions that access S3
def aws_retry(f):
    def retry_wrapper(self, *args, **kwargs):
        last_exc = None
        for i in range(S3_NUM_RETRIES):
            try:
                return f(self, *args, **kwargs)
            except MetaflowException as ex:
                # MetaflowExceptions are not related to AWS, don't retry
                raise
            except Exception as ex:
                try:
                    function_name = f.func_name
                except AttributeError:
                    function_name = f.__name__
                sys.stderr.write("S3 datastore operation %s failed (%s). "
                                 "Retrying %d more times..\n"
                                 % (function_name, ex, S3_NUM_RETRIES - i))
                self.reset_client(hard_reset=True)
                last_exc = ex
                # exponential backoff
                time.sleep(2**i + random.randint(0, 5))
        raise last_exc
    return retry_wrapper
