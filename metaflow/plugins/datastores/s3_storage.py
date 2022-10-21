import os

from itertools import starmap

from metaflow.plugins.datatools.s3.s3 import S3, S3Client, S3PutObject
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3, ARTIFACT_LOCALROOT
from metaflow.datastore.datastore_storage import CloseAfterUse, DataStoreStorage


try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse


class S3Storage(DataStoreStorage):
    TYPE = "s3"

    def __init__(self, root=None):
        super(S3Storage, self).__init__(root)
        self.s3_client = S3Client()

    @classmethod
    def get_datastore_root_from_config(cls, echo, create_on_absent=True):
        return DATASTORE_SYSROOT_S3

    def is_file(self, paths):
        with S3(
            s3root=self.datastore_root,
            tmproot=ARTIFACT_LOCALROOT,
            external_client=self.s3_client,
        ) as s3:
            if len(paths) > 10:
                s3objs = s3.info_many(paths, return_missing=True)
                return [s3obj.exists for s3obj in s3objs]
            else:
                result = []
                for path in paths:
                    result.append(s3.info(path, return_missing=True).exists)
                return result

    def info_file(self, path):
        with S3(
            s3root=self.datastore_root,
            tmproot=ARTIFACT_LOCALROOT,
            external_client=self.s3_client,
        ) as s3:
            s3obj = s3.info(path, return_missing=True)
            return s3obj.exists, s3obj.metadata

    def size_file(self, path):
        with S3(
            s3root=self.datastore_root,
            tmproot=ARTIFACT_LOCALROOT,
            external_client=self.s3_client,
        ) as s3:
            s3obj = s3.info(path, return_missing=True)
            return s3obj.size

    def list_content(self, paths):
        strip_prefix_len = len(self.datastore_root.rstrip("/")) + 1
        with S3(
            s3root=self.datastore_root,
            tmproot=ARTIFACT_LOCALROOT,
            external_client=self.s3_client,
        ) as s3:
            results = s3.list_paths(paths)
            return [
                self.list_content_result(
                    path=o.url[strip_prefix_len:], is_file=o.exists
                )
                for o in results
            ]

    def save_bytes(self, path_and_bytes_iter, overwrite=False, len_hint=0):
        def _convert():
            # Output format is the same as what is needed for S3PutObject:
            # key, value, path, content_type, metadata
            for path, obj in path_and_bytes_iter:
                if isinstance(obj, tuple):
                    yield path, obj[0], None, None, obj[1]
                else:
                    yield path, obj, None, None, None

        with S3(
            s3root=self.datastore_root,
            tmproot=ARTIFACT_LOCALROOT,
            external_client=self.s3_client,
        ) as s3:
            # HACK: The S3 datatools we rely on does not currently do a good job
            # determining if uploading things in parallel is more efficient than
            # serially. We use a heuristic for now where if we have a lot of
            # files, we will go in parallel and if we have few files, we will
            # serially upload them. This is not ideal because there is also a size
            # factor and one very large file with a few other small files, for
            # example, would benefit from a parallel upload.
            #
            # In the case of save_artifacts, currently len_hint is based on the
            # total number of artifacts, not taking into account how many of them
            # already exist in the CAS, i.e. it can be a gross overestimate. As a
            # result, it is possible we take a latency hit by using put_many only
            # for a few artifacts.
            #
            # A better approach would be to e.g. write all blobs to temp files
            # and based on the total size and number of files use either put_files
            # (which avoids re-writing the files) or s3.put sequentially.
            if len_hint > 10:
                # Use put_many
                s3.put_many(starmap(S3PutObject, _convert()), overwrite)
            else:
                # Sequential upload
                for key, obj, _, _, metadata in _convert():
                    s3.put(key, obj, overwrite=overwrite, metadata=metadata)

    def load_bytes(self, paths):
        if len(paths) == 0:
            return CloseAfterUse(iter([]))

        s3 = S3(
            s3root=self.datastore_root,
            tmproot=ARTIFACT_LOCALROOT,
            external_client=self.s3_client,
        )

        def iter_results():
            # We similarly do things in parallel for many files. This is again
            # a hack.
            if len(paths) > 10:
                results = s3.get_many(paths, return_missing=True, return_info=True)
                for r in results:
                    if r.exists:
                        yield r.key, r.path, r.metadata
                    else:
                        yield r.key, None, None
            else:
                for p in paths:
                    r = s3.get(p, return_missing=True, return_info=True)
                    if r.exists:
                        yield r.key, r.path, r.metadata
                    else:
                        yield r.key, None, None

        return CloseAfterUse(iter_results(), closer=s3)
