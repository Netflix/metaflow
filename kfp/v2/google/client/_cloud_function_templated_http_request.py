# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud function that proxies HTTP request after replacing placeholders."""

import datetime
import json
import logging
import re
import traceback
from typing import Optional, Tuple

from google import auth
from google.auth.transport import urllib3


def _process_request(request):
    """Wrapper of _process_request_impl with error handling."""
    try:
        return _process_request_impl(request)
    except Exception as err:
        traceback.print_exc()
        raise err


def _preprocess_request_body(
    request_body: bytes,
    time: datetime.datetime,
) -> Tuple[str, str, Optional[bytes]]:
    """Augments the request body before sending it to CAIP Pipelines API.

    Replaces placeholders, generates unique name, removes `_discovery_url`.
    Args:
      request_body: Request body
      time: The scheduled invocation time.

    Returns:
      Tuple of (url, method, resolved_request_body).
    """
    request_str = request_body.decode('utf-8')

    # Replacing placeholders like: {{$.scheduledTime.strftime('%Y-%m-%d')}}
    request_str = re.sub(r"{{\$.scheduledTime.strftime\('([^']*)'\)}}",
                         lambda m: time.strftime(m.group(1)), request_str)

    request_json = json.loads(request_str)

    url = str(request_json['_url'])
    del request_json['_url']

    method = str(request_json['_method'])
    del request_json['_method']

    resolved_request_body = None
    if request_json:
        resolved_request_str = json.dumps(request_json)
        resolved_request_body = resolved_request_str.encode('utf-8')
    return (url, method, resolved_request_body)


def _process_request_impl(request):
    """Processes the incoming HTTP request.

    Args:
      request (flask.Request): HTTP request object.

    Returns:
      The response text or any set of values that can be turned into a Response
      object using `make_response
      <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    time = datetime.datetime.now()

    logging.debug('request.headers=%s', request.headers)
    logging.debug('Original request body: %s', request.data)
    (url, method, resolved_request_body) = _preprocess_request_body(
        request_body=request.data,
        time=time,
    )
    logging.debug('url=%s', url)
    logging.debug('method=%s', method)
    logging.debug('Resolved request body: %s', resolved_request_body)

    credentials, _ = auth.default()
    authorized_http = urllib3.AuthorizedHttp(credentials=credentials)
    response = authorized_http.request(
        url=url,
        method=method,
        body=resolved_request_body,
    )
    data_str = response.data.decode('utf-8')
    if response.status != 200:
        print(f'response.status={response.status}')
        print(f'response.data={data_str}')
    return (response.data, response.status)
