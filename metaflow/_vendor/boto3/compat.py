# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import sys
import os
import errno
import socket
import warnings

from metaflow._vendor.botocore.vendored import six
from metaflow._vendor.boto3.exceptions import PythonDeprecationWarning

if six.PY3:
    # In python3, socket.error is OSError, which is too general
    # for what we want (i.e FileNotFoundError is a subclass of OSError).
    # In py3 all the socket related errors are in a newly created
    # ConnectionError
    SOCKET_ERROR = ConnectionError
else:
    SOCKET_ERROR = socket.error

if six.PY3:
    import collections.abc as collections_abc
else:
    import collections as collections_abc


if sys.platform.startswith('win'):
    def rename_file(current_filename, new_filename):
        try:
            os.remove(new_filename)
        except OSError as e:
            if not e.errno == errno.ENOENT:
                # We only want to a ignore trying to remove
                # a file that does not exist.  If it fails
                # for any other reason we should be propagating
                # that exception.
                raise
        os.rename(current_filename, new_filename)
else:
    rename_file = os.rename


def filter_python_deprecation_warnings():
    """
    Invoking this filter acknowledges your runtime will soon be deprecated
    at which time you will stop receiving all updates to your client.
    """
    warnings.filterwarnings(
        'ignore',
        message=".*Boto3 will no longer support Python.*",
        category=PythonDeprecationWarning,
        module=r".*boto3\.compat"
    )


def _warn_deprecated_python():
    py_34_35_params = {
        'date': 'February 1, 2021',
        'blog_link': 'https://aws.amazon.com/blogs/developer/announcing-'
                     'the-end-of-support-for-python-3-4-and-3-5-in-the-'
                     'aws-sdk-for-python-and-aws-cli-v1/'
    }
    py_27_params = {
        'date': 'July 15, 2021',
        'blog_link': 'https://aws.amazon.com/blogs/developer/announcing-end-'
                     'of-support-for-python-2-7-in-aws-sdk-for-python-and-'
                     'aws-cli-v1/'
    }
    deprecated_versions = {
        (3,4): py_34_35_params,
        (3,5): py_34_35_params,
        (2,7): py_27_params,
    }
    py_version = sys.version_info[:2]

    if py_version in deprecated_versions:
        params = deprecated_versions[py_version]
        warning = (
            "Boto3 will no longer support Python {}.{} "
            "starting {}. To continue receiving service updates, "
            "bug fixes, and security updates please upgrade to Python 3.6 or "
            "later. More information can be found here: {}"
        ).format(py_version[0], py_version[1], params['date'], params['blog_link'])
        warnings.warn(warning, PythonDeprecationWarning)
