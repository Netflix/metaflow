import os
import re
import tarfile

from distutils.dir_util import copy_tree
try:
    # python2
    from urlparse import urlparse
except:  # noqa E722
    # python3
    from urllib.parse import urlparse

from metaflow import util
from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.local import LocalDataStore
from metaflow.datastore.util.s3util import get_s3_client

def sync_metadata_to_S3(metadata_local_dir, datastore_root, retry_count):
    with util.TempDir() as td:
        tar_file_path = os.path.join(td, 'metadata.tgz')
        with tarfile.open(tar_file_path, 'w:gz') as tar:
            tar.add(metadata_local_dir)
        # Upload metadata to Amazon S3.
        with open(tar_file_path, 'rb') as f:
            s3, _ = get_s3_client()
            url = urlparse(
                        os.path.join(
                            datastore_root,
                            MetaflowDataStore.filename_with_attempt_prefix(
                                'metadata.tgz',
                                retry_count)))
            s3.upload_fileobj(f, url.netloc, url.path.lstrip('/'))  

def sync_metadata_from_S3(metadata_local_dir, datastore_root, retry_count):
    def echo_none(*args, **kwargs):
        pass
    url = urlparse(
                os.path.join(
                    datastore_root,
                    MetaflowDataStore.filename_with_attempt_prefix(
                        'metadata.tgz',
                        retry_count)))
    s3, err = get_s3_client()
    try:
        s3.head_object(Bucket=url.netloc, Key=url.path.lstrip('/'))
        with util.TempDir() as td:
            tar_file_path = os.path.join(td, 'metadata.tgz')
            with open(tar_file_path, 'wb') as f:
                s3.download_fileobj(url.netloc, url.path.lstrip('/'), f)
            with tarfile.open(tar_file_path, 'r:gz') as tar:
                tar.extractall(td)
            copy_tree(
                os.path.join(td, metadata_local_dir),
                LocalDataStore.get_datastore_root_from_config(echo_none),
                update=True)
    except err as e:
        # Metadata sync is best effort.
        pass

def get_docker_registry(image_uri):
    """
    Explanation:
        (.+?(?:[:.].+?)\/)? - [GROUP 0] REGISTRY
            .+?                 - A registry must start with at least one character
            (?:[:.].+?)\/       - A registry must have ":" or "." and end with "/"
            ?                   - Make a registry optional
        (.*?)               - [GROUP 1] REPOSITORY
            .*?                 - Get repository name until separator
        (?:[@:])?           - SEPARATOR
            ?:                  - Don't capture separator
            [@:]                - The separator must be either "@" or ":"
            ?                   - The separator is optional
        ((?<=[@:]).*)?      - [GROUP 2] TAG / DIGEST
            (?<=[@:])           - A tag / digest must be preceeded by "@" or ":"
            .*                  - Capture rest of tag / digest
            ?                   - A tag / digest is optional
    Examples:
        image
            - None
            - image
            - None
        example/image
            - None
            - example/image
            - None
        example/image:tag
            - None
            - example/image
            - tag
        example.domain.com/example/image:tag
            - example.domain.com/
            - example/image
            - tag
        123.123.123.123:123/example/image:tag
            - 123.123.123.123:123/
            - example/image
            - tag
        example.domain.com/example/image@sha256:45b23dee0
            - example.domain.com/
            - example/image
            - sha256:45b23dee0
    """

    pattern = re.compile(r"^(.+?(?:[:.].+?)\/)?(.*?)(?:[@:])?((?<=[@:]).*)?$")
    registry, repository, tag = pattern.match(image_uri).groups()
    if registry is not None:
        registry = registry.rstrip("/")
    return registry