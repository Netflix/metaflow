# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account
import requests_toolbelt.adapters.appengine
from webbrowser import open_new_tab
import requests
import json

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
LOCAL_KFP_CREDENTIAL = os.path.expanduser('~/.config/kfp/credentials.json')


def get_gcp_access_token():
    """Get and return GCP access token for the current Application Default
    Credentials.

    If not set, returns None. For more information, see
    https://cloud.google.com/sdk/gcloud/reference/auth/application-default/print-access-token
    """
    token = None
    try:
        creds, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        if not creds.valid:
            auth_req = Request()
            creds.refresh(auth_req)
        if creds.valid:
            token = creds.token
    except Exception as e:
        logging.warning('Failed to get GCP access token: %s', e)
    return token


def get_auth_token(client_id, other_client_id, other_client_secret):
    """Gets auth token from default service account or user account."""
    if os.path.exists(LOCAL_KFP_CREDENTIAL):
        # fetch IAP auth token using the locally stored credentials.
        with open(LOCAL_KFP_CREDENTIAL, 'r') as f:
            credentials = json.load(f)
        if client_id in credentials:
            return id_token_from_refresh_token(
                credentials[client_id]['other_client_id'],
                credentials[client_id]['other_client_secret'],
                credentials[client_id]['refresh_token'], client_id)
    if other_client_id is None or other_client_secret is None:
        # fetch IAP auth token: service accounts
        token = get_auth_token_from_sa(client_id)
    else:
        # fetch IAP auth token: user account
        # Obtain the ID token for provided Client ID with user accounts.
        #  Flow: get authorization code -> exchange for refresh token -> obtain and return ID token
        refresh_token = get_refresh_token_from_client_id(
            other_client_id, other_client_secret)
        credentials = {}
        if os.path.exists(LOCAL_KFP_CREDENTIAL):
            with open(LOCAL_KFP_CREDENTIAL, 'r') as f:
                credentials = json.load(f)
        credentials[client_id] = {}
        credentials[client_id]['other_client_id'] = other_client_id
        credentials[client_id]['other_client_secret'] = other_client_secret
        credentials[client_id]['refresh_token'] = refresh_token
        #TODO: handle the case when the refresh_token expires.
        #   which only happens if the refresh_token is not used once for six months.
        if not os.path.exists(os.path.dirname(LOCAL_KFP_CREDENTIAL)):
            os.makedirs(os.path.dirname(LOCAL_KFP_CREDENTIAL))
        with open(LOCAL_KFP_CREDENTIAL, 'w') as f:
            json.dump(credentials, f)
        token = id_token_from_refresh_token(other_client_id,
                                            other_client_secret, refresh_token,
                                            client_id)
    return token


def get_auth_token_from_sa(client_id):
    """Gets auth token from default service account.

    If no service account credential is found, returns None.
    """
    service_account_credentials = get_service_account_credentials(client_id)
    if service_account_credentials:
        return get_google_open_id_connect_token(service_account_credentials)
    return None


def get_service_account_credentials(client_id):
    # Figure out what environment we're running in and get some preliminary
    # information about the service account.
    bootstrap_credentials, _ = google.auth.default(scopes=[IAM_SCOPE])
    if isinstance(bootstrap_credentials, google.oauth2.credentials.Credentials):
        logging.info('Found OAuth2 credentials and skip SA auth.')
        return None
    elif isinstance(bootstrap_credentials, google.auth.app_engine.Credentials):
        requests_toolbelt.adapters.appengine.monkeypatch()

    # For service account's using the Compute Engine metadata service,
    # service_account_email isn't available until refresh is called.
    bootstrap_credentials.refresh(Request())
    signer_email = bootstrap_credentials.service_account_email
    if isinstance(bootstrap_credentials,
                  google.auth.compute_engine.credentials.Credentials):
        # Since the Compute Engine metadata service doesn't expose the service
        # account key, we use the IAM signBlob API to sign instead.
        # In order for this to work:
        #
        # 1. Your VM needs the https://www.googleapis.com/auth/iam scope.
        #    You can specify this specific scope when creating a VM
        #    through the API or gcloud. When using Cloud Console,
        #    you'll need to specify the "full access to all Cloud APIs"
        #    scope. A VM's scopes can only be specified at creation time.
        #
        # 2. The VM's default service account needs the "Service Account Actor"
        #    role. This can be found under the "Project" category in Cloud
        #    Console, or roles/iam.serviceAccountActor in gcloud.
        signer = google.auth.iam.Signer(Request(), bootstrap_credentials,
                                        signer_email)
    else:
        # A Signer object can sign a JWT using the service account's key.
        signer = bootstrap_credentials.signer

    # Construct OAuth 2.0 service account credentials using the signer
    # and email acquired from the bootstrap credentials.
    return google.oauth2.service_account.Credentials(
        signer,
        signer_email,
        token_uri=OAUTH_TOKEN_URI,
        additional_claims={'target_audience': client_id})


def get_google_open_id_connect_token(service_account_credentials):
    """Get an OpenID Connect token issued by Google for the service account.

    This function:
      1. Generates a JWT signed with the service account's private key
         containing a special "target_audience" claim.
      2. Sends it to the OAUTH_TOKEN_URI endpoint. Because the JWT in #1
         has a target_audience claim, that endpoint will respond with
         an OpenID Connect token for the service account -- in other words,
         a JWT signed by *Google*. The aud claim in this JWT will be
         set to the value from the target_audience claim in #1.
    For more information, see
    https://developers.google.com/identity/protocols/OAuth2ServiceAccount .
    The HTTP/REST example on that page describes the JWT structure and
    demonstrates how to call the token endpoint. (The example on that page
    shows how to get an OAuth2 access token; this code is using a
    modified version of it to get an OpenID Connect token.)
    """

    service_account_jwt = (
        service_account_credentials._make_authorization_grant_assertion())
    request = google.auth.transport.requests.Request()
    body = {
        'assertion': service_account_jwt,
        'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(
        request, OAUTH_TOKEN_URI, body)
    return token_response['id_token']


def get_refresh_token_from_client_id(client_id, client_secret):
    """Obtain the ID token for provided Client ID with user accounts.

    Flow: get authorization code -> exchange for refresh token -> obtain and return ID token
    """
    auth_code = get_auth_code(client_id)
    return get_refresh_token_from_code(auth_code, client_id, client_secret)


def get_auth_code(client_id):
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?client_id=%s&response_type=code&scope=openid%%20email&access_type=offline&redirect_uri=urn:ietf:wg:oauth:2.0:oob" % client_id
    print(auth_url)
    open_new_tab(auth_url)
    return input(
        "If there's no browser window prompt, please direct to the URL above, then copy and paste the authorization code here: "
    )


def get_refresh_token_from_code(auth_code, client_id, client_secret):
    payload = {
        "code": auth_code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "authorization_code"
    }
    res = requests.post(OAUTH_TOKEN_URI, data=payload)
    res.raise_for_status()
    return str(json.loads(res.text)[u"refresh_token"])


def id_token_from_refresh_token(client_id, client_secret, refresh_token,
                                audience):
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "audience": audience
    }
    res = requests.post(OAUTH_TOKEN_URI, data=payload)
    res.raise_for_status()
    return str(json.loads(res.text)[u"id_token"])
