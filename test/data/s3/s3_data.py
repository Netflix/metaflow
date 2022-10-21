import os
import sys
import zlib
import json
from uuid import uuid4
from hashlib import sha1
from collections import namedtuple

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse

from metaflow.plugins.datatools.s3 import S3PutObject

from metaflow.util import to_fileobj, to_bytes, url_quote

import numpy

from .. import s3client, S3ROOT

BASIC_METADATA = {
    "no_meta": (None, None),  # No metadata at all but going through the calls
    "content_no_meta": ("text/plain", None),  # Content-type but no metadata
    "no_content_meta": (None, {"userkey": "UserValue"}),  # No content-type but metadata
    "isolation": (
        "text/plain",
        {"content-type": "text/css"},
    ),  # Check isolation of user metadata
    "multiple": (
        "text/plain",
        {"userkey1": "UserValue1", "userkey2": "UserValue2"},
    ),  # Multiple metadata
    "complex": (
        "text/plain",
        {
            "utf8-data": "\u523a\u8eab/means sashimi",
            "with-weird-chars": "Space and !@#<>:/-+=&%",
        },
    ),
}

BASIC_RANGE_INFO = {
    "from_beg": (0, 16),  # From beginning
    "exceed_end": (0, 10 * 1024**3),  # From beginning, should fetch full file
    "middle": (5, 10),  # From middle
    "end": (None, -5),  # Fetch from end
    "till_end": (5, None),  # Fetch till end
}

# None for file size denotes missing keys
# To properly support ranges in a useful manner, make files at least 32 bytes
# long
BASIC_DATA = [
    # empty prefixes should not be a problem
    ("empty_prefix", {}),
    # requesting non-existent data should be handled ok
    ("missing_files", {"missing": None}),
    # a basic sanity check
    (
        "3_small_files",
        {"empty_file": 0, "kb_file": 1024, "mb_file": 1024**2, "missing_file": None},
    ),
    # S3 paths can be longer than the max allowed filename on Linux
    (
        "long_path",
        {
            "/".join("x" * 300): 1024,
            # one medium-size path for list_path test
            "/".join("y" * 10): 32,
            "x/x/x": None,
        },
    ),
    # test that nested prefixes work correctly
    (
        "prefix",
        {
            "prefix": 32,
            "prefixprefix": 33,
            # note that prefix/prefix is both an object and a prefix
            "prefix/prefix": 34,
            "prefix/prefix/prefix": None,
        },
    ),
    # same filename as above but a different prefix
    ("samefile", {"prefix": 42, "x": 43, "empty_file": 1, "xx": None}),
    # crazy file names (it seems '#' characters don't work with boto)
    (
        "crazypath",
        {
            "crazy spaces": 34,
            "\x01\xff": 64,
            "\u523a\u8eab/means sashimi": 33,
            "crazy-!.$%@2_()\"'": 100,
            " /cra._:zy/\x01\x02/p a t h/$this/!!is()": 1000,
            "crazy missing :(": None,
        },
    ),
]

BIG_DATA = [
    # test a file > 4GB
    ("5gb_file", {"5gb_file": 5 * 1024**3}),
    # ensure that e.g. paged listings work correctly with many keys
    ("3000_files", {str(i): i for i in range(3000)}),
]

# Large file to use for benchmark, must be in BASIC_DATA or BIG_DATA
BENCHMARK_SMALL_FILE = ("3000_files", {"1": 1})
BENCHMARK_MEDIUM_FILE = ("3_small_files", {"mb_file": 1024**2})
BENCHMARK_LARGE_FILE = ("5gb_file", {"5gb_file": 5 * 1024**3})

BENCHMARK_SMALL_ITER_MAX = 10001
BENCHMARK_MEDIUM_ITER_MAX = 501
BENCHMARK_LARGE_ITER_MAX = 11

FAKE_RUN_DATA = [
    # test a run id - just a random run id
    ("HelloFlow/56", {"one_a": 512, "one_b": 1024, "two_c": 8192})
]

PUT_PREFIX = "put_tests"

ExpectedResult = namedtuple(
    "ExpectedResult", "size checksum content_type metadata range"
)

ExpectedRange = namedtuple(
    "ExpectedRange", "total_size result_offset result_size req_offset req_size"
)


class RandomFile(object):

    cached_digests = {}
    cached_files = {}

    def __init__(self, prefix, fname, size):
        self.key = os.path.join(prefix, fname)
        self.prefix = prefix
        self.fname = fname
        self.size = size
        self._data = None

    def _make_data(self):
        numpy.random.seed(zlib.adler32(self.key.encode("utf-8")) & 0xFFFFFFFF)
        self._data = numpy.random.bytes(self.size)

    def checksum(self, start=None, length=None):
        if self.size is not None:
            start = start if start else 0
            length = length if length and start + length < self.size else self.size
            lookup_key = "%s:%d:%d" % (self.key, start, length)
            if lookup_key not in self.cached_digests:
                if self._data is None:
                    self._make_data()
                if length < 0:
                    self.cached_digests[lookup_key] = sha1(
                        self._data[length:]
                    ).hexdigest()
                else:
                    self.cached_digests[lookup_key] = sha1(
                        self._data[start : start + length]
                    ).hexdigest()
            return self.cached_digests[lookup_key]

    def size_from_range(self, start, length):
        if self.size is None:
            return None, None
        if length:
            if length > 0:
                end = length + start
            else:
                assert start is None
                start = self.size + length
                end = self.size
        else:
            end = self.size

        if end > self.size:
            end = self.size
        if start >= end:
            return None, None
        return end - start, start

    def fileobj(self):
        if self.size is not None:
            return to_fileobj(self.data)

    @property
    def data(self):
        if self._data is None and self.size is not None:
            self._make_data()
        return self._data

    @property
    def url(self):
        return os.path.join(S3ROOT, self.key)


def _format_test_cases(dataset, meta=None, ranges=None):
    cases = []
    ids = []
    for prefix, filespecs in dataset:
        objs = [RandomFile(prefix, fname, size) for fname, size in filespecs.items()]
        objs = {obj.url: (obj, None, None) for obj in objs}
        if meta:
            # We generate one per meta info
            for metaname, (content_type, usermeta) in meta.items():
                objs.update(
                    {
                        "%s_%s" % (obj.url, metaname): (obj, content_type, usermeta)
                        for (obj, _, _) in objs.values()
                    }
                )
        files = {
            k: {
                None: ExpectedResult(
                    size=obj.size,
                    checksum=obj.checksum(),
                    content_type=content_type,
                    metadata=usermeta,
                    range=None,
                )
            }
            for k, (obj, content_type, usermeta) in objs.items()
        }
        if ranges:
            # For every file we have in files, we calculate the proper
            # checksum and create a new dictionary
            for k, (obj, content_type, usermeta) in objs.items():
                for offset, length in ranges.values():
                    expected_size, real_offset = obj.size_from_range(offset, length)
                    if expected_size is None or expected_size > obj.size:
                        continue
                    files[k][(offset, length)] = ExpectedResult(
                        size=expected_size,
                        checksum=obj.checksum(offset, length),
                        content_type=content_type,
                        metadata=usermeta,
                        range=ExpectedRange(
                            total_size=obj.size,
                            result_offset=real_offset,
                            result_size=expected_size,
                            req_offset=offset,
                            req_size=length,
                        ),
                    )

        ids.append(prefix)
        cases.append((S3ROOT, [prefix], files))
    return cases, ids


def pytest_fakerun_cases():
    cases, ids = _format_test_cases(FAKE_RUN_DATA)
    return {"argvalues": cases, "ids": ids}


def pytest_basic_case():
    cases, ids = _format_test_cases(
        BASIC_DATA, ranges=BASIC_RANGE_INFO, meta=BASIC_METADATA
    )
    return {"argvalues": cases, "ids": ids}


def pytest_large_case():
    cases, ids = _format_test_cases(BASIC_DATA, meta=BASIC_METADATA)
    cases_big, ids_big = _format_test_cases(BIG_DATA)
    cases.extend(cases_big)
    ids.extend(ids_big)
    return {"argvalues": cases, "ids": ids}


def pytest_benchmark_case():
    cases, _ = _format_test_cases([BENCHMARK_LARGE_FILE])
    ids = ["5gb"]

    new_cases, _ = _format_test_cases([BENCHMARK_MEDIUM_FILE])
    cases.extend(new_cases)
    ids.append("1mb")

    new_cases, _ = _format_test_cases([BENCHMARK_SMALL_FILE])
    cases.extend(new_cases)
    ids.append("1b")

    return {"argvalues": cases, "ids": ids}


def pytest_benchmark_many_case():
    large_case = _format_test_cases([BENCHMARK_LARGE_FILE])[0][0]
    medium_case = _format_test_cases([BENCHMARK_MEDIUM_FILE])[0][0]
    small_case = _format_test_cases([BENCHMARK_SMALL_FILE])[0][0]

    # Configuration: we will form groups of up to BENCHMARK_*_ITER_MAX items
    # (count taken from iteration_count). We will also form groups taking from
    # all three sets
    cases = []
    ids = []
    iteration_count = [0, 1, 10, 50, 500, 10000]
    for small_count in iteration_count:
        if small_count > BENCHMARK_SMALL_ITER_MAX:
            break
        for medium_count in iteration_count:
            if medium_count > BENCHMARK_MEDIUM_ITER_MAX:
                break
            for large_count in iteration_count:
                if large_count > BENCHMARK_LARGE_ITER_MAX:
                    break
                if small_count + medium_count + large_count == 0:
                    continue
                # At this point, form the test
                id_name = "%ds_%dm_%dl" % (small_count, medium_count, large_count)
                cases.append(
                    (
                        S3ROOT,
                        [],
                        [
                            (small_count, small_case[2]),
                            (medium_count, medium_case[2]),
                            (large_count, large_case[2]),
                        ],
                    )
                )
                ids.append(id_name)
    return {"argvalues": cases, "ids": ids}


def pytest_benchmark_put_case():
    put_prefix = os.path.join(S3ROOT, PUT_PREFIX)
    cases = []
    ids = []
    for prefix, filespecs in [
        BENCHMARK_LARGE_FILE,
        BENCHMARK_MEDIUM_FILE,
        BENCHMARK_SMALL_FILE,
    ]:
        blobs = []
        for fname, size in filespecs.items():
            blobs.append((prefix, fname, size))
        cases.append((put_prefix, blobs, None))
    ids = ["5gb", "1mb", "1b"]
    return {"argvalues": cases, "ids": ids}


def pytest_benchmark_put_many_case():
    single_cases_and_ids = pytest_benchmark_put_case()
    single_cases = single_cases_and_ids["argvalues"]
    large_blob = single_cases[0][1][0]
    medium_blob = single_cases[1][1][0]
    small_blob = single_cases[2][1][0]
    put_prefix = os.path.join(S3ROOT, PUT_PREFIX)
    # Configuration: we will form groups of up to BENCHMARK_*_ITER_MAX items
    # (count taken from iteration_count). We will also form groups taking from
    # all three sets
    cases = []
    ids = []
    iteration_count = [0, 1, 10, 50, 500, 10000]
    for small_count in iteration_count:
        if small_count > BENCHMARK_SMALL_ITER_MAX:
            break
        for medium_count in iteration_count:
            if medium_count > BENCHMARK_MEDIUM_ITER_MAX:
                break
            for large_count in iteration_count:
                if large_count > BENCHMARK_LARGE_ITER_MAX:
                    break
                if small_count + medium_count + large_count == 0:
                    continue
                # At this point, form the test
                id_name = "%ds_%dm_%dl" % (small_count, medium_count, large_count)
                blobs = [
                    (small_count, small_blob),
                    (medium_count, medium_blob),
                    (large_count, large_blob),
                ]
                cases.append((put_prefix, blobs, None))
                ids.append(id_name)
    return {"argvalues": cases, "ids": ids}


def pytest_many_prefixes_case():
    cases, ids = _format_test_cases(BASIC_DATA, meta=BASIC_METADATA)
    many_prefixes = []
    many_prefixes_expected = {}
    for s3root, [prefix], files in cases:
        many_prefixes.append(prefix)
        many_prefixes_expected.update(files)
    # add many prefixes cases
    ids.append("many_prefixes")
    cases.append((S3ROOT, many_prefixes, many_prefixes_expected))
    return {"argvalues": cases, "ids": ids}


def pytest_put_strings_case(meta=None):
    put_prefix = os.path.join(S3ROOT, PUT_PREFIX)
    data = [
        "unicode: \u523a\u8eab means sashimi",
        b"bytes: \x00\x01\x02",
        "just a string",
    ]
    expected = {}
    objs = []
    for text in data:
        blob = to_bytes(text)
        checksum = sha1(blob).hexdigest()
        key = str(uuid4())
        expected[os.path.join(put_prefix, key)] = {
            None: ExpectedResult(
                size=len(blob),
                checksum=checksum,
                content_type=None,
                metadata=None,
                range=None,
            )
        }
        objs.append((key, text))
        if meta is not None:
            for content_type, usermeta in meta.values():
                key = str(uuid4())
                expected[os.path.join(put_prefix, key)] = {
                    None: ExpectedResult(
                        size=len(blob),
                        checksum=checksum,
                        content_type=content_type,
                        metadata=usermeta,
                        range=None,
                    )
                }
                objs.append(
                    S3PutObject(
                        key=key,
                        value=text,
                        content_type=content_type,
                        metadata=usermeta,
                    )
                )
    return {"argvalues": [(put_prefix, objs, expected)], "ids": ["put_strings"]}


def pytest_put_blobs_case(meta=None):
    put_prefix = os.path.join(S3ROOT, PUT_PREFIX)
    cases = []
    ids = []
    for prefix, filespecs in BIG_DATA:
        expected = {}
        blobs = []
        for fname, size in filespecs.items():
            blob = RandomFile(prefix, fname, size)
            checksum = blob.checksum()
            key = str(uuid4())
            expected[os.path.join(put_prefix, key)] = {
                None: ExpectedResult(
                    size=blob.size,
                    checksum=checksum,
                    content_type=None,
                    metadata=None,
                    range=None,
                )
            }
            blobs.append((key, blob.data))
            if meta is not None:
                for content_type, usermeta in meta.values():
                    key = str(uuid4())
                    expected[os.path.join(put_prefix, key)] = {
                        None: ExpectedResult(
                            size=len(blob),
                            checksum=checksum,
                            content_type=content_type,
                            metadata=usermeta,
                            range=None,
                        )
                    }
                    blobs.append(
                        S3PutObject(
                            key=key,
                            value=blob.data,
                            content_type=content_type,
                            metadata=usermeta,
                        )
                    )
        ids.append(prefix)
        cases.append((put_prefix, blobs, expected))
    return {"argvalues": cases, "ids": ids}


def ensure_test_data():
    # update S3ROOT in __init__.py to get a fresh set of data
    print("Ensuring that test data exists at %s" % S3ROOT)
    mark = urlparse(os.path.join(S3ROOT, "ALL_OK"))
    try:
        # Check if the data exists and has been modified in the last
        # 29 days (this should be lower than the TTL for your bucket to ensure
        # the data is available for the test)
        import datetime

        today = datetime.date.today()
        delta = datetime.timedelta(days=29)
        s3client.head_object(
            Bucket=mark.netloc,
            Key=mark.path.lstrip("/"),
            IfModifiedSince=str(today - delta),
        )
        print("All data ok.")
    except:
        print("Uploading test data")

        def _do_upload(prefix, filespecs, meta=None):
            for fname, size in filespecs.items():
                if size is not None:
                    f = RandomFile(prefix, fname, size)
                    url = urlparse(f.url)
                    # For metadata, we don't actually touch RandomFile
                    # (since it is the same) but we modify the path to post-pend
                    # the name
                    print("Test data case %s: upload to %s started" % (prefix, f.url))
                    s3client.upload_fileobj(
                        f.fileobj(), url.netloc, url.path.lstrip("/")
                    )
                    print("Test data case %s: uploaded to %s" % (prefix, f.url))
                    if meta is not None:
                        for metaname, metainfo in meta.items():
                            new_url = "%s_%s" % (f.url, metaname)
                            url = urlparse(new_url)
                            print(
                                "Test data case %s: upload to %s started"
                                % (prefix, new_url)
                            )
                            extra = {}
                            content_type, user_meta = metainfo
                            if content_type:
                                extra["ContentType"] = content_type
                            if user_meta:
                                new_meta = {
                                    "metaflow-user-attributes": json.dumps(user_meta)
                                }
                                extra["Metadata"] = new_meta
                            s3client.upload_fileobj(
                                f.fileobj(),
                                url.netloc,
                                url.path.lstrip("/"),
                                ExtraArgs=extra,
                            )
                            print(
                                "Test data case %s: uploaded to %s" % (prefix, new_url)
                            )

        for prefix, filespecs in BIG_DATA + FAKE_RUN_DATA:
            _do_upload(prefix, filespecs)
        for prefix, filespecs in BASIC_DATA:
            _do_upload(prefix, filespecs, meta=BASIC_METADATA)

        s3client.upload_fileobj(
            to_fileobj("ok"), Bucket=mark.netloc, Key=mark.path.lstrip("/")
        )
        print("Test data uploaded ok")


ensure_test_data()
