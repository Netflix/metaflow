# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
"""Internal module to help with normalizing botocore client args.

This module (and all function/classes within this module) should be
considered internal, and *not* a public API.

"""
import copy
import logging
import socket

import botocore.exceptions
import botocore.serialize
import botocore.utils
from metaflow._vendor.botocore.signers import RequestSigner
from metaflow._vendor.botocore.config import Config
from metaflow._vendor.botocore.endpoint import EndpointCreator


logger = logging.getLogger(__name__)


VALID_REGIONAL_ENDPOINTS_CONFIG = [
    'legacy',
    'regional',
]
LEGACY_GLOBAL_STS_REGIONS = [
    'ap-northeast-1',
    'ap-south-1',
    'ap-southeast-1',
    'ap-southeast-2',
    'aws-global',
    'ca-central-1',
    'eu-central-1',
    'eu-north-1',
    'eu-west-1',
    'eu-west-2',
    'eu-west-3',
    'sa-east-1',
    'us-east-1',
    'us-east-2',
    'us-west-1',
    'us-west-2',
]


class ClientArgsCreator(object):
    def __init__(self, event_emitter, user_agent, response_parser_factory,
                 loader, exceptions_factory, config_store):
        self._event_emitter = event_emitter
        self._user_agent = user_agent
        self._response_parser_factory = response_parser_factory
        self._loader = loader
        self._exceptions_factory = exceptions_factory
        self._config_store = config_store

    def get_client_args(self, service_model, region_name, is_secure,
                        endpoint_url, verify, credentials, scoped_config,
                        client_config, endpoint_bridge):
        final_args = self.compute_client_args(
            service_model, client_config, endpoint_bridge, region_name,
            endpoint_url, is_secure, scoped_config)

        service_name = final_args['service_name']
        parameter_validation = final_args['parameter_validation']
        endpoint_config = final_args['endpoint_config']
        protocol = final_args['protocol']
        config_kwargs = final_args['config_kwargs']
        s3_config = final_args['s3_config']
        partition = endpoint_config['metadata'].get('partition', None)
        socket_options = final_args['socket_options']

        signing_region = endpoint_config['signing_region']
        endpoint_region_name = endpoint_config['region_name']

        event_emitter = copy.copy(self._event_emitter)
        signer = RequestSigner(
            service_model.service_id, signing_region,
            endpoint_config['signing_name'],
            endpoint_config['signature_version'],
            credentials, event_emitter
        )

        config_kwargs['s3'] = s3_config
        new_config = Config(**config_kwargs)
        endpoint_creator = EndpointCreator(event_emitter)

        endpoint = endpoint_creator.create_endpoint(
            service_model, region_name=endpoint_region_name,
            endpoint_url=endpoint_config['endpoint_url'], verify=verify,
            response_parser_factory=self._response_parser_factory,
            max_pool_connections=new_config.max_pool_connections,
            proxies=new_config.proxies,
            timeout=(new_config.connect_timeout, new_config.read_timeout),
            socket_options=socket_options,
            client_cert=new_config.client_cert,
            proxies_config=new_config.proxies_config)

        serializer = botocore.serialize.create_serializer(
            protocol, parameter_validation)
        response_parser = botocore.parsers.create_parser(protocol)
        return {
            'serializer': serializer,
            'endpoint': endpoint,
            'response_parser': response_parser,
            'event_emitter': event_emitter,
            'request_signer': signer,
            'service_model': service_model,
            'loader': self._loader,
            'client_config': new_config,
            'partition': partition,
            'exceptions_factory': self._exceptions_factory
        }

    def compute_client_args(self, service_model, client_config,
                            endpoint_bridge, region_name, endpoint_url,
                            is_secure, scoped_config):
        service_name = service_model.endpoint_prefix
        protocol = service_model.metadata['protocol']
        parameter_validation = True
        if client_config and not client_config.parameter_validation:
            parameter_validation = False
        elif scoped_config:
            raw_value = scoped_config.get('parameter_validation')
            if raw_value is not None:
                parameter_validation = botocore.utils.ensure_boolean(raw_value)

        # Override the user agent if specified in the client config.
        user_agent = self._user_agent
        if client_config is not None:
            if client_config.user_agent is not None:
                user_agent = client_config.user_agent
            if client_config.user_agent_extra is not None:
                user_agent += ' %s' % client_config.user_agent_extra

        s3_config = self.compute_s3_config(client_config)
        endpoint_config = self._compute_endpoint_config(
            service_name=service_name,
            region_name=region_name,
            endpoint_url=endpoint_url,
            is_secure=is_secure,
            endpoint_bridge=endpoint_bridge,
            s3_config=s3_config,
        )
        # Create a new client config to be passed to the client based
        # on the final values. We do not want the user to be able
        # to try to modify an existing client with a client config.
        config_kwargs = dict(
            region_name=endpoint_config['region_name'],
            signature_version=endpoint_config['signature_version'],
            user_agent=user_agent)
        if client_config is not None:
            config_kwargs.update(
                connect_timeout=client_config.connect_timeout,
                read_timeout=client_config.read_timeout,
                max_pool_connections=client_config.max_pool_connections,
                proxies=client_config.proxies,
                proxies_config=client_config.proxies_config,
                retries=client_config.retries,
                client_cert=client_config.client_cert,
                inject_host_prefix=client_config.inject_host_prefix,
            )
        self._compute_retry_config(config_kwargs)
        s3_config = self.compute_s3_config(client_config)
        return {
            'service_name': service_name,
            'parameter_validation': parameter_validation,
            'user_agent': user_agent,
            'endpoint_config': endpoint_config,
            'protocol': protocol,
            'config_kwargs': config_kwargs,
            's3_config': s3_config,
            'socket_options': self._compute_socket_options(scoped_config)
        }

    def compute_s3_config(self, client_config):
        s3_configuration = self._config_store.get_config_variable('s3')

        # Next specific client config values takes precedence over
        # specific values in the scoped config.
        if client_config is not None:
            if client_config.s3 is not None:
                if s3_configuration is None:
                    s3_configuration = client_config.s3
                else:
                    # The current s3_configuration dictionary may be
                    # from a source that only should be read from so
                    # we want to be safe and just make a copy of it to modify
                    # before it actually gets updated.
                    s3_configuration = s3_configuration.copy()
                    s3_configuration.update(client_config.s3)

        return s3_configuration

    def _compute_endpoint_config(self, service_name, region_name, endpoint_url,
                                 is_secure, endpoint_bridge, s3_config):
        resolve_endpoint_kwargs = {
            'service_name': service_name,
            'region_name': region_name,
            'endpoint_url': endpoint_url,
            'is_secure': is_secure,
            'endpoint_bridge': endpoint_bridge,
        }
        if service_name == 's3':
            return self._compute_s3_endpoint_config(
                s3_config=s3_config, **resolve_endpoint_kwargs)
        if service_name == 'sts':
            return self._compute_sts_endpoint_config(**resolve_endpoint_kwargs)
        return self._resolve_endpoint(**resolve_endpoint_kwargs)

    def _compute_s3_endpoint_config(self, s3_config,
                                    **resolve_endpoint_kwargs):
        force_s3_global = self._should_force_s3_global(
            resolve_endpoint_kwargs['region_name'], s3_config)
        if force_s3_global:
            resolve_endpoint_kwargs['region_name'] = None
        endpoint_config = self._resolve_endpoint(**resolve_endpoint_kwargs)
        self._set_region_if_custom_s3_endpoint(
            endpoint_config, resolve_endpoint_kwargs['endpoint_bridge'])
        # For backwards compatibility reasons, we want to make sure the
        # client.meta.region_name will remain us-east-1 if we forced the
        # endpoint to be the global region. Specifically, if this value
        # changes to aws-global, it breaks logic where a user is checking
        # for us-east-1 as the global endpoint such as in creating buckets.
        if force_s3_global and endpoint_config['region_name'] == 'aws-global':
            endpoint_config['region_name'] = 'us-east-1'
        return endpoint_config

    def _should_force_s3_global(self, region_name, s3_config):
        s3_regional_config = 'legacy'
        if s3_config and 'us_east_1_regional_endpoint' in s3_config:
            s3_regional_config = s3_config['us_east_1_regional_endpoint']
            self._validate_s3_regional_config(s3_regional_config)
        return (
            s3_regional_config == 'legacy' and
            region_name in ['us-east-1', None]
        )

    def _validate_s3_regional_config(self, config_val):
        if config_val not in VALID_REGIONAL_ENDPOINTS_CONFIG:
            raise botocore.exceptions.\
                InvalidS3UsEast1RegionalEndpointConfigError(
                    s3_us_east_1_regional_endpoint_config=config_val)

    def _set_region_if_custom_s3_endpoint(self, endpoint_config,
                                          endpoint_bridge):
        # If a user is providing a custom URL, the endpoint resolver will
        # refuse to infer a signing region. If we want to default to s3v4,
        # we have to account for this.
        if endpoint_config['signing_region'] is None \
                and endpoint_config['region_name'] is None:
            endpoint = endpoint_bridge.resolve('s3')
            endpoint_config['signing_region'] = endpoint['signing_region']
            endpoint_config['region_name'] = endpoint['region_name']

    def _compute_sts_endpoint_config(self, **resolve_endpoint_kwargs):
        endpoint_config = self._resolve_endpoint(**resolve_endpoint_kwargs)
        if self._should_set_global_sts_endpoint(
                resolve_endpoint_kwargs['region_name'],
                resolve_endpoint_kwargs['endpoint_url']):
            self._set_global_sts_endpoint(
                endpoint_config, resolve_endpoint_kwargs['is_secure'])
        return endpoint_config

    def _should_set_global_sts_endpoint(self, region_name, endpoint_url):
        if endpoint_url:
            return False
        return (
            self._get_sts_regional_endpoints_config() == 'legacy' and
            region_name in LEGACY_GLOBAL_STS_REGIONS
        )

    def _get_sts_regional_endpoints_config(self):
        sts_regional_endpoints_config = self._config_store.get_config_variable(
            'sts_regional_endpoints')
        if not sts_regional_endpoints_config:
            sts_regional_endpoints_config = 'legacy'
        if sts_regional_endpoints_config not in \
                VALID_REGIONAL_ENDPOINTS_CONFIG:
            raise botocore.exceptions.InvalidSTSRegionalEndpointsConfigError(
                sts_regional_endpoints_config=sts_regional_endpoints_config)
        return sts_regional_endpoints_config

    def _set_global_sts_endpoint(self, endpoint_config, is_secure):
        scheme = 'https' if is_secure else 'http'
        endpoint_config['endpoint_url'] = '%s://sts.amazonaws.com' % scheme
        endpoint_config['signing_region'] = 'us-east-1'

    def _resolve_endpoint(self, service_name, region_name,
                          endpoint_url, is_secure, endpoint_bridge):
        return endpoint_bridge.resolve(
            service_name, region_name, endpoint_url, is_secure)

    def _compute_socket_options(self, scoped_config):
        # This disables Nagle's algorithm and is the default socket options
        # in urllib3.
        socket_options = [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
        if scoped_config:
            # Enables TCP Keepalive if specified in shared config file.
            if self._ensure_boolean(scoped_config.get('tcp_keepalive', False)):
                socket_options.append(
                    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
        return socket_options

    def _compute_retry_config(self, config_kwargs):
        self._compute_retry_max_attempts(config_kwargs)
        self._compute_retry_mode(config_kwargs)

    def _compute_retry_max_attempts(self, config_kwargs):
        # There's a pre-existing max_attempts client config value that actually
        # means max *retry* attempts.  There's also a `max_attempts` we pull
        # from the config store that means *total attempts*, which includes the
        # intitial request.  We can't change what `max_attempts` means in
        # client config so we try to normalize everything to a new
        # "total_max_attempts" variable.  We ensure that after this, the only
        # configuration for "max attempts" is the 'total_max_attempts' key.
        # An explicitly provided max_attempts in the client config
        # overrides everything.
        retries = config_kwargs.get('retries')
        if retries is not None:
            if 'total_max_attempts' in retries:
                retries.pop('max_attempts', None)
                return
            if 'max_attempts' in retries:
                value = retries.pop('max_attempts')
                # client config max_attempts means total retries so we
                # have to add one for 'total_max_attempts' to account
                # for the initial request.
                retries['total_max_attempts'] = value + 1
                return
        # Otherwise we'll check the config store which checks env vars,
        # config files, etc.  There is no default value for max_attempts
        # so if this returns None and we don't set a default value here.
        max_attempts = self._config_store.get_config_variable('max_attempts')
        if max_attempts is not None:
            if retries is None:
                retries = {}
                config_kwargs['retries'] = retries
            retries['total_max_attempts'] = max_attempts

    def _compute_retry_mode(self, config_kwargs):
        retries = config_kwargs.get('retries')
        if retries is None:
            retries = {}
            config_kwargs['retries'] = retries
        elif 'mode' in retries:
            # If there's a retry mode explicitly set in the client config
            # that overrides everything.
            return
        retry_mode = self._config_store.get_config_variable('retry_mode')
        if retry_mode is None:
            retry_mode = 'legacy'
        retries['mode'] = retry_mode

    def _ensure_boolean(self, val):
        if isinstance(val, bool):
            return val
        else:
            return val.lower() == 'true'
