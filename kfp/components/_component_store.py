__all__ = [
    'ComponentStore',
]

from pathlib import Path
import copy
import hashlib
import json
import logging
import requests
import tempfile
from typing import Callable, Iterable
from uritemplate import URITemplate
from . import _components as comp
from .structures import ComponentReference
from ._key_value_store import KeyValueStore

_COMPONENT_FILENAME = 'component.yaml'


class ComponentStore:
    """Component store.
    
    Enables external components to be loaded by name and digest/tag.
    
    Attributes:

        local_search_paths: A list of local directories to include in the search.
        url_seach_prefixes: A list of URL prefixes to include in the search.
        uri_search_template: A URI template for components, which may include {name}, {digest} and {tag} variables.
    """

    def __init__(self,
                 local_search_paths=None,
                 url_search_prefixes=None,
                 auth=None,
                 uri_search_template=None):
        """Instantiates a ComponentStore including the specified locations.
        
        Args:

            local_search_paths: A list of local directories to include in the search.
            url_seach_prefixes: A list of URL prefixes to include in the search.
            auth: Auth object for the requests library. See https://requests.readthedocs.io/en/master/user/authentication/
            uri_search_template: A URI template for components, which may include {name}, {digest} and {tag} variables.
        """
        self.local_search_paths = local_search_paths or ['.']
        if uri_search_template:
            self.uri_search_template = URITemplate(uri_search_template)
        self.url_search_prefixes = url_search_prefixes or []
        self._auth = auth

        self._component_file_name = 'component.yaml'
        self._digests_subpath = 'versions/sha256'
        self._tags_subpath = 'versions/tags'

        cache_base_dir = Path(tempfile.gettempdir()) / '.kfp_components'
        self._git_blob_hash_to_data_db = KeyValueStore(
            cache_dir=cache_base_dir / 'git_blob_hash_to_data')
        self._url_to_info_db = KeyValueStore(cache_dir=cache_base_dir /
                                             'url_to_info')

    def load_component_from_url(self, url):
        """Loads a component from a URL.

        Args:
            url: The url of the component specification.

        Returns:
            A factory function with a strongly-typed signature.
        """
        return comp.load_component_from_url(url=url, auth=self._auth)

    def load_component_from_file(self, path):
        """Loads a component from a path.

        Args:
            path: The path of the component specification.

        Returns:
            A factory function with a strongly-typed signature.
        """
        return comp.load_component_from_file(path)

    def load_component(self, name, digest=None, tag=None):
        """Loads component local file or URL and creates a task factory
        function.

        Search locations:

        * :code:`<local-search-path>/<name>/component.yaml`
        * :code:`<url-search-prefix>/<name>/component.yaml`

        If the digest is specified, then the search locations are:

        * :code:`<local-search-path>/<name>/versions/sha256/<digest>`
        * :code:`<url-search-prefix>/<name>/versions/sha256/<digest>`

        If the tag is specified, then the search locations are:

        * :code:`<local-search-path>/<name>/versions/tags/<digest>`
        * :code:`<url-search-prefix>/<name>/versions/tags/<digest>`

        Args:
            name:   Component name used to search and load the component artifact containing the component definition.
                    Component name usually has the following form: group/subgroup/component
            digest: Strict component version. SHA256 hash digest of the component artifact file. Can be used to load a specific component version so that the pipeline is reproducible.
            tag:    Version tag. Can be used to load component version from a specific branch. The version of the component referenced by a tag can change in future.

        Returns:
            A factory function with a strongly-typed signature.
            Once called with the required arguments, the factory constructs a pipeline task instance (ContainerOp).
        """
        #This function should be called load_task_factory since it returns a factory function.
        #The real load_component function should produce an object with component properties (e.g. name, description, inputs/outputs).
        #TODO: Change this function to return component spec object but it should be callable to construct tasks.
        component_ref = ComponentReference(name=name, digest=digest, tag=tag)
        component_ref = self._load_component_spec_in_component_ref(
            component_ref)
        return comp._create_task_factory_from_component_spec(
            component_spec=component_ref.spec,
            component_ref=component_ref,
        )

    def _load_component_spec_in_component_ref(
        self,
        component_ref: ComponentReference,
    ) -> ComponentReference:
        """Takes component_ref, finds the component spec and returns
        component_ref with .spec set to the component spec.

        See ComponentStore.load_component for the details of the search
        logic.
        """
        if component_ref.spec:
            return component_ref

        component_ref = copy.copy(component_ref)
        if component_ref.url:
            component_ref.spec = comp._load_component_spec_from_url(
                url=component_ref.url, auth=self._auth)
            return component_ref

        name = component_ref.name
        if not name:
            raise TypeError("name is required")
        if name.startswith('/') or name.endswith('/'):
            raise ValueError(
                'Component name should not start or end with slash: "{}"'
                .format(name))

        digest = component_ref.digest
        tag = component_ref.tag

        tried_locations = []

        if digest is not None and tag is not None:
            raise ValueError('Cannot specify both tag and digest')

        if digest is not None:
            path_suffix = name + '/' + self._digests_subpath + '/' + digest
        elif tag is not None:
            path_suffix = name + '/' + self._tags_subpath + '/' + tag
            #TODO: Handle symlinks in GIT URLs
        else:
            path_suffix = name + '/' + self._component_file_name

        #Trying local search paths
        for local_search_path in self.local_search_paths:
            component_path = Path(local_search_path, path_suffix)
            tried_locations.append(str(component_path))
            if component_path.is_file():
                # TODO: Verify that the content matches the digest (if specified).
                component_ref._local_path = str(component_path)
                component_ref.spec = comp._load_component_spec_from_file(
                    str(component_path))
                return component_ref

        #Trying URI template
        if self.uri_search_template:
            url = self.uri_search_template.expand(
                name=name, digest=digest, tag=tag)
            tried_locations.append(url)
            if self._load_component_spec_from_url(component_ref, url):
                return component_ref

        #Trying URL prefixes
        for url_search_prefix in self.url_search_prefixes:
            url = url_search_prefix + path_suffix
            tried_locations.append(url)
            if self._load_component_spec_from_url(component_ref, url):
                return component_ref

        raise RuntimeError(
            'Component {} was not found. Tried the following locations:\n{}'
            .format(name, '\n'.join(tried_locations)))

    def _load_component_spec_from_url(self, component_ref, url) -> bool:
        """Loads component spec from a URL.

        On success, the url and spec attributes of the component_ref arg will be populated.

        Args:
            component_ref: the component whose spec to load.
            url: the location from which to obtain the component spec.

        Returns:
            True if the component was retrieved and non-empty; otherwise False.
        """

        try:
            response = requests.get(
                url, auth=self._auth
            )  #Does not throw exceptions on bad status, but throws on dead domains and malformed URLs. Should we log those cases?
            response.raise_for_status()
        except:
            return False

        if response.content:
            # TODO: Verify that the content matches the digest (if specified).
            component_ref.url = url
            component_ref.spec = comp._load_component_spec_from_yaml_or_zip_bytes(
                response.content)
            return True

        return False

    def _load_component_from_ref(self,
                                 component_ref: ComponentReference) -> Callable:
        component_ref = self._load_component_spec_in_component_ref(
            component_ref)
        return comp._create_task_factory_from_component_spec(
            component_spec=component_ref.spec, component_ref=component_ref)

    def search(self, name: str):
        """Searches for components by name in the configured component store.

        Prints the component name and URL for components that match the given name.
        Only components on GitHub are currently supported.

        Example::

            kfp.components.ComponentStore.default_store.search('xgboost')

            # Returns results:
            #     Xgboost train   https://raw.githubusercontent.com/.../components/XGBoost/Train/component.yaml
            #     Xgboost predict https://raw.githubusercontent.com/.../components/XGBoost/Predict/component.yaml
        """
        self._refresh_component_cache()
        for url in self._url_to_info_db.keys():
            component_info = json.loads(
                self._url_to_info_db.try_get_value_bytes(url))
            component_name = component_info['name']
            if name.casefold() in component_name.casefold():
                print('\t'.join([
                    component_name,
                    url,
                ]))

    def list(self):
        self.search('')

    def _refresh_component_cache(self):
        for url_search_prefix in self.url_search_prefixes:
            if url_search_prefix.startswith(
                    'https://raw.githubusercontent.com/'):
                logging.info('Searching for components in "{}"'.format(
                    url_search_prefix))
                for candidate in _list_candidate_component_uris_from_github_repo(
                        url_search_prefix, auth=self._auth):
                    component_url = candidate['url']
                    if self._url_to_info_db.exists(component_url):
                        continue

                    logging.debug(
                        'Found new component URL: "{}"'.format(component_url))

                    blob_hash = candidate['git_blob_hash']
                    if not self._git_blob_hash_to_data_db.exists(blob_hash):
                        logging.debug(
                            'Downloading component spec from "{}"'.format(
                                component_url))
                        response = _get_request_session().get(
                            component_url, auth=self._auth)
                        response.raise_for_status()
                        component_data = response.content

                        # Verifying the hash
                        received_data_hash = _calculate_git_blob_hash(
                            component_data)
                        if received_data_hash.lower() != blob_hash.lower():
                            raise RuntimeError(
                                'The downloaded component ({}) has incorrect hash: "{}" != "{}"'
                                .format(
                                    component_url,
                                    received_data_hash,
                                    blob_hash,
                                ))

                        # Verifying that the component is loadable
                        try:
                            component_spec = comp._load_component_spec_from_component_text(
                                component_data)
                        except:
                            continue
                        self._git_blob_hash_to_data_db.store_value_bytes(
                            blob_hash, component_data)
                    else:
                        component_data = self._git_blob_hash_to_data_db.try_get_value_bytes(
                            blob_hash)
                        component_spec = comp._load_component_spec_from_component_text(
                            component_data)

                    component_name = component_spec.name
                    self._url_to_info_db.store_value_text(
                        component_url,
                        json.dumps(
                            dict(
                                name=component_name,
                                url=component_url,
                                git_blob_hash=blob_hash,
                                digest=_calculate_component_digest(
                                    component_data),
                            )))


def _get_request_session(max_retries: int = 3):
    session = requests.Session()

    retry_strategy = requests.packages.urllib3.util.retry.Retry(
        total=max_retries,
        backoff_factor=0.1,
        status_forcelist=[413, 429, 500, 502, 503, 504],
        method_whitelist=frozenset(['GET', 'POST']),
    )

    session.mount('https://',
                  requests.adapters.HTTPAdapter(max_retries=retry_strategy))
    session.mount('http://',
                  requests.adapters.HTTPAdapter(max_retries=retry_strategy))

    return session


def _calculate_git_blob_hash(data: bytes) -> str:
    return hashlib.sha1(b'blob ' + str(len(data)).encode('utf-8') + b'\x00' +
                        data).hexdigest()


def _calculate_component_digest(data: bytes) -> str:
    return hashlib.sha256(data.replace(b'\r\n', b'\n')).hexdigest()


def _list_candidate_component_uris_from_github_repo(url_search_prefix: str,
                                                    auth=None) -> Iterable[str]:
    (schema, _, host, org, repo, ref,
     path_prefix) = url_search_prefix.split('/', 6)
    for page in range(1, 999):
        search_url = (
            'https://api.github.com/search/code?q=filename:{}+repo:{}/{}&page={}&per_page=1000'
        ).format(_COMPONENT_FILENAME, org, repo, page)
        response = _get_request_session().get(search_url, auth=auth)
        response.raise_for_status()
        result = response.json()
        items = result['items']
        if not items:
            break
        for item in items:
            html_url = item['html_url']
            # Constructing direct content URL
            # There is an API (/repos/:owner/:repo/git/blobs/:file_sha) for
            # getting the blob content, but it requires decoding the content.
            raw_url = html_url.replace(
                'https://github.com/',
                'https://raw.githubusercontent.com/').replace('/blob/', '/', 1)
            if not raw_url.endswith(_COMPONENT_FILENAME):
                # GitHub matches component_test.yaml when searching for filename:"component.yaml"
                continue
            result_item = dict(
                url=raw_url,
                path=item['path'],
                git_blob_hash=item['sha'],
            )
            yield result_item


ComponentStore.default_store = ComponentStore(
    local_search_paths=[
        '.',
    ],
    uri_search_template='https://raw.githubusercontent.com/kubeflow/pipelines/{tag}/components/{name}/component.yaml',
    url_search_prefixes=[
        'https://raw.githubusercontent.com/kubeflow/pipelines/master/components/'
    ],
)
