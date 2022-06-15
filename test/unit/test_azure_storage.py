import hashlib
import io
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from azure.core.credentials import AccessToken

from metaflow.datastore import get_datastore_impl
from metaflow.datastore.azure_exceptions import (
    MetaflowAzureResourceError,
    MetaflowAzureAuthenticationError,
)
from metaflow.datastore.azure_storage import (
    parse_azure_sysroot,
)

import unittest
import inspect

# This container should exist, and we should be able to access it
CONTAINER_FOR_TEST = "test-azure-storage"


class TestAzureStorage(unittest.TestCase):
    # This class is 90% generic across all datastores. We should generalize it fully, so we can run
    # it against S3. Potentially use it to develop the next storage implementation too.
    maxDiff = None

    @staticmethod
    def generate_datastore_root():
        return "%s/%d_%s" % (
            CONTAINER_FOR_TEST,
            time.time(),
            str(uuid.uuid4()),
        )

    def setUp(self):
        self.storage = get_datastore_impl("azure")(
            TestAzureStorage.generate_datastore_root()
        )

    def tearDown(self):
        azure_client_maker = self.storage._get_client()
        container = azure_client_maker.get_blob_container_client()
        _, blob_prefix = parse_azure_sysroot(self.storage.datastore_root)
        # delete_blob(s) did not work well...when there was lots of stuff to delete.
        blobs_to_delete = list(container.list_blobs(blob_prefix))
        if len(blobs_to_delete) > 0:
            print("Deleting (%d) test blobs" % len(blobs_to_delete))
            futures = []
            with ThreadPoolExecutor(max_workers=32) as t:
                for bp in blobs_to_delete:
                    # Comment this line to leave test blobs for debugging
                    futures.append(t.submit(container.delete_blob, bp.name))
            for f in futures:
                f.result()
            print("Done")
        self.storage = None

    def test_list_content(self):
        list_content_scenarios = [
            {
                "keys_to_exist": [
                    "test_list_content/a/b",
                    "test_list_content/c/d",
                    "test_list_content/e",
                ],
                "queries": [
                    {
                        "paths": ["test_list_content/"],
                        "expected_result": [
                            ("test_list_content/a", False),
                            ("test_list_content/c", False),
                            ("test_list_content/e", True),
                        ],
                    },
                    {
                        # a is missing trailing slash on purpose
                        "paths": ["test_list_content/a", "test_list_content/c/"],
                        "expected_result": [
                            ("test_list_content/a/b", True),
                            ("test_list_content/c/d", True),
                        ],
                    },
                    {
                        # list content at a non-existent folder
                        "paths": [hashlib.md5(os.urandom(32)).hexdigest()],
                        "expected_result": [],
                    },
                ],
            }
        ]
        for scenario in list_content_scenarios:
            keys_to_exist = scenario["keys_to_exist"]
            self.storage.save_bytes([(k, io.BytesIO(b"")) for k in keys_to_exist])

            for query in scenario["queries"]:
                actual_result = self.storage.list_content(query["paths"])
                actual_result = sorted((r.path, r.is_file) for r in actual_result)
                expected_result = sorted(query["expected_result"])
                self.assertListEqual(expected_result, actual_result)

    def test_size_file(self):
        self.storage.save_bytes(
            [
                ("zero_bytes", io.BytesIO(b"")),
                ("forty_two_bytes", io.BytesIO(b"8" * 42)),
            ],
        )
        self.assertEqual(self.storage.size_file("zero_bytes"), 0)

        self.assertEqual(self.storage.size_file("forty_two_bytes"), 42)

    def test_is_file(self):
        self.storage.save_bytes([("test_is_file/a_file", io.BytesIO(b""))])

        self.assertListEqual(
            self.storage.is_file(
                [
                    "test_is_file/a_file",
                    "test_is_file/MISSING_FILE",
                    "test_is_file",
                    "test_is_file/",
                ]
            ),
            [True, False, False, False],
        )

    def test_load_bytes(self):
        """Make sure load_bytes works for existing AND missing paths"""
        paths_to_save = []
        paths_to_load = []
        nonce = str(uuid.uuid4())
        for i in range(42):
            path = "%s/%d" % (nonce, i)
            paths_to_load.append(path)
            if i % 2 == 0:
                paths_to_save.append(path)
        items_to_save = [(path, io.BytesIO(b"")) for path in paths_to_save]
        self.storage.save_bytes(items_to_save)

        nones_seen = 0
        with self.storage.load_bytes(paths_to_load) as load_result:
            for key, tmpfile, metadata in load_result:
                if key in paths_to_save:
                    with open(tmpfile, "rb") as f:
                        self.assertEqual(f.read(), b"")
                else:
                    self.assertIsNone(tmpfile)
                    nones_seen += 1
        self.assertGreater(
            nones_seen, 0, "We should have at least one None for test efficacy"
        )
        self.assertEqual(nones_seen, len(paths_to_load) - len(paths_to_save))

    def test_save_load_bytes(self):
        save_load_cases = [
            {
                "bytes_by_key": {
                    "test_save_load_bytes/aa/1": "床前明月光".encode(),
                    "test_save_load_bytes/aa/2": "疑是地上霜".encode(),
                    "test_save_load_bytes/aa/3": "舉頭望明月".encode(),
                    "test_save_load_bytes/aa/4": "低頭思故鄉".encode(),
                }
            }
        ]
        for case in save_load_cases:
            bytes_to_save = case["bytes_by_key"]
            self.storage.save_bytes(
                [
                    # we also generate a metadata dict (third component below)
                    (k, (io.BytesIO(v), {"metadata_key": k}))
                    for k, v in bytes_to_save.items()
                ],
            )
            bytes_loaded = {}
            metadatas_loaded = {}

            all_tmp_files = []
            with self.storage.load_bytes(bytes_to_save.keys()) as load_result:
                for key, tmpfile, metadata in load_result:
                    with open(tmpfile, "rb") as f:
                        bytes_loaded[key] = f.read()
                    all_tmp_files.append(tmpfile)
                    metadatas_loaded[key] = metadata

            # verify all tmp files deleted as expected
            self.assertTrue(all(not os.path.exists(t) for t in all_tmp_files))

            # verify we load what we save
            self.assertDictEqual(bytes_loaded, bytes_to_save)

            expected_metadatas = {}
            for k in bytes_loaded:
                expected_metadatas[k] = {"metadata_key": k}
            self.assertDictEqual(metadatas_loaded, expected_metadatas)

    def test_save_bytes_overwrite(self):
        self.assertFalse(self.storage.is_file(["a"])[0])
        self.storage.save_bytes([("a", io.BytesIO(b""))])
        self.assertTrue(self.storage.is_file(["a"])[0])
        self.storage.save_bytes([("a", io.BytesIO(b"new content"))], overwrite=False)
        a_content = None
        with self.storage.load_bytes(["a"]) as load_result:
            for _, tmpfile, _ in load_result:
                with open(tmpfile, "rb") as f:
                    a_content = f.read()
        self.assertEqual(a_content, b"")

        # Now let's allow overwrite
        self.storage.save_bytes([("a", io.BytesIO(b"new content"))], overwrite=True)
        with self.storage.load_bytes(["a"]) as load_result:
            for _, tmpfile, _ in load_result:
                with open(tmpfile, "rb") as f:
                    a_content = f.read()
        self.assertEqual(a_content, b"new content")

    def test_save_load_bytes_many(self):
        key_set = {str(uuid.uuid4()) for _ in range(256)}
        self.storage.save_bytes([(k, io.BytesIO(k.encode())) for k in key_set])
        with self.storage.load_bytes(key_set) as load_result:
            for key, tmpfile, metadata in load_result:
                self.assertIn(key, key_set)
                with open(tmpfile, "rb") as f:
                    self.assertEqual(f.read(), key.encode())
                key_set.remove(key)
        self.assertEqual(len(key_set), 0)

    def test_is_file_many_check_client_cache(self):
        self.storage = get_datastore_impl(ds_type="azure")(
            TestAzureStorage.generate_datastore_root()
        )
        from metaflow.datastore.azure_storage import BlobServiceClientCache

        # we reset the client cache, because we want to strictly check cache size later
        BlobServiceClientCache._cache = dict()

        key_set = {str(uuid.uuid4()) for _ in range(256)}
        self.storage.is_file(key_set)

        # We are running with thread pool.  We expect the client cache to contain N entries where
        # N is the max number of workers in the thread pool.
        if self.storage._use_processes:
            self.assertEqual(len(BlobServiceClientCache._cache), 0)
        else:
            self.assertEqual(
                len(BlobServiceClientCache._cache), max(1, os.cpu_count() // 2)
            )

    def test_bad_credentials(self):
        self.storage._default_scope_token = AccessToken(
            # bad token value
            token="not a real token",
            # but, not expired (give it a year)
            expires_on=int(time.time() + 365 * 24 * 3600),
        )
        old_access_key_value = os.getenv("METAFLOW_AZURE_STORAGE_ACCESS_KEY")
        try:
            if old_access_key_value is not None:
                del os.environ["METAFLOW_AZURE_STORAGE_ACCESS_KEY"]
            with self.assertRaises(MetaflowAzureAuthenticationError):
                self.storage.list_content(["a"])

            # clear this up, so that we know to get a new, valid token during tearDown
        finally:
            if old_access_key_value is not None:
                os.environ["METAFLOW_AZURE_STORAGE_ACCESS_KEY"] = old_access_key_value
            self.storage._default_scope_token = None

    def test_parse_azure_sysroot(self):
        cases = [
            {"sysroot": "container", "parse_result": ("container", None)},
            {
                "sysroot": "container/a",
                "parse_result": ("container", "a"),
            },
            {
                "sysroot": "container/a/b",
                "parse_result": ("container", "a/b"),
            },
            {"sysroot": "/container/", "parse_result": ValueError},
            {"sysroot": "container/", "parse_result": ValueError},
            {
                "sysroot": "",
                "parse_result": ValueError,
            },
            {
                "sysroot": "container/a//b",
                "parse_result": ValueError,
            },
        ]
        for case in cases:
            if inspect.isclass(case["parse_result"]) and issubclass(
                case["parse_result"], Exception
            ):
                with self.assertRaises(
                    case["parse_result"], msg="failing case: " + str(case)
                ):
                    print(parse_azure_sysroot(case["sysroot"]))
            else:
                self.assertTupleEqual(
                    parse_azure_sysroot(case["sysroot"]),
                    case["parse_result"],
                    "failing case: " + str(case),
                )


# Test cases that would be good to add next:
# - token expiration (_Credential) - pull it out for unit test
# - more test cases for list_content
# - verify strong consistency: do many save/delete cycles on the same file
#
# Also, really try to get rid of those crazy ResourceWarnings (unittest seems to open the floodgates)

if __name__ == "__main__":
    unittest.main()
