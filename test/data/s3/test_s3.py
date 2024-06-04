import os
from re import I
import shutil
from hashlib import sha1
from tempfile import mkdtemp
from itertools import groupby, starmap
import random
from uuid import uuid4
from metaflow.plugins.datatools import s3

import pytest

from metaflow import current, namespace, Run
from metaflow.plugins.datatools.s3 import (
    S3,
    MetaflowS3AccessDenied,
    MetaflowS3NotFound,
    MetaflowS3URLException,
    MetaflowS3InvalidObject,
    S3PutObject,
    S3GetObject,
)

from metaflow.util import to_bytes, unicode_type

from . import s3_data
from .. import FakeFlow, DO_TEST_RUN

try:
    # python2
    from urlparse import urlparse
except:
    # python3
    from urllib.parse import urlparse


def s3_get_object_from_url_range(url, range_info):
    if range_info is None:
        return S3GetObject(url, None, None)
    return S3GetObject(url, range_info.req_offset, range_info.req_size)


def assert_results(
    s3objs,
    expected,
    info_should_be_empty=False,
    info_only=False,
    ranges_fetched=None,
    encryption=None,
):
    # did we receive all expected objects and nothing else?
    if info_only:
        info_should_be_empty = False
    if ranges_fetched is None:
        ranges_fetched = [None] * len(s3objs)
    assert len(s3objs) == len(ranges_fetched)

    for s3obj, range_info in zip(s3objs, ranges_fetched):
        # assert that all urls returned are unicode, if not None
        assert isinstance(s3obj.key, (unicode_type, type(None)))
        assert isinstance(s3obj.url, (unicode_type, type(None)))
        assert isinstance(s3obj.prefix, (unicode_type, type(None)))

        # is key actually a suffix?
        assert s3obj.url.endswith(s3obj.key)
        if s3obj.prefix:
            # is prefix actually a prefix?
            assert s3obj.url.startswith(s3obj.prefix)
            # key must look like a real key
            assert 0 < len(s3obj.key) < len(s3obj.url)
        else:
            # if there's no prefix, the key is the url
            assert s3obj.url == s3obj.key

        if range_info:
            expected_result = expected[s3obj.url].get(
                (range_info.req_offset, range_info.req_size)
            )
        else:
            expected_result = expected[s3obj.url].get(None)
        assert expected_result
        size = expected_result.size
        checksum = expected_result.checksum
        content_type = expected_result.content_type
        metadata = expected_result.metadata
        range_to_match = expected_result.range
        if size is None:
            assert s3obj.exists is False
            assert s3obj.downloaded is False
        else:
            assert s3obj.exists is True
            if info_only:
                assert s3obj.downloaded is False
            else:
                assert s3obj.downloaded is True
                # local file exists?
                assert os.path.exists(s3obj.path)
                # blob is ok?
                blob = s3obj.blob
                assert len(blob) == size
                assert type(blob) == type(b"")
                assert sha1(blob).hexdigest() == checksum
            # size is ok?
            assert s3obj.size == size
            if info_should_be_empty:
                assert not s3obj.has_info
            else:
                assert s3obj.has_info
                # Content_type is OK
                if content_type is None:
                    # Default content-type when nothing is supplied
                    assert s3obj.content_type == "binary/octet-stream"
                else:
                    assert s3obj.content_type == content_type
                # Range information is properly reported. Note that in this case, even
                # for whole files we return a range. Ranges don't exist for just information
                # on files
                if not info_only:
                    s3obj_range_info = s3obj.range_info
                    assert s3obj_range_info is not None
                    if range_to_match is None:
                        # This is the entire file
                        assert s3obj_range_info.total_size == size
                        assert s3obj_range_info.request_offset == 0
                        assert s3obj_range_info.request_length == size
                    else:
                        # A specific range of the file
                        assert s3obj_range_info.total_size == range_to_match.total_size
                        assert (
                            s3obj_range_info.request_offset
                            == range_to_match.result_offset
                        )
                        assert (
                            s3obj_range_info.request_length
                            == range_to_match.result_size
                        )
                # metadata is OK
                if metadata is None:
                    assert s3obj.metadata is None
                else:
                    s3objmetadata = s3obj.metadata
                    assert s3objmetadata is not None
                    found = set()
                    for k, v in metadata.items():
                        v1 = s3objmetadata.get(k, None)
                        assert v1 == v, "Metadata %s mismatch" % k
                        found.add(k)
                    extra_keys = set(s3objmetadata.keys()) - found
                    assert not extra_keys, "Additional metadata present %s" % str(
                        extra_keys
                    )
                # if encryption was used
                if encryption:
                    assert s3obj.encryption == encryption


def shuffle(objs):
    for i, (key, value) in enumerate(objs):
        t = random.randrange(i, len(objs))
        key_t, value_t = objs[t]
        objs[i], objs[t] = (key, value_t), (key_t, value)


def deranged_shuffle(objs):
    shuffled_objs = objs[:]
    while True:
        shuffle(shuffled_objs)
        for (i, a), (j, b) in zip(objs, shuffled_objs):
            if a == b:
                break
        else:
            return shuffled_objs


@pytest.fixture
def tempdir():
    tmpdir = mkdtemp(dir=".", prefix="metaflow.test.tmp")
    yield tmpdir
    shutil.rmtree(tmpdir)


@pytest.mark.parametrize(
    argnames=["s3root", "pathspecs", "expected"], **s3_data.pytest_benchmark_case()
)
@pytest.mark.benchmark(max_time=30)
def test_info_one_benchmark(benchmark, s3root, pathspecs, expected):
    def _do():
        with S3() as s3:
            res = []
            for url in expected:
                res.append(s3.info(url))
            return res

    res = benchmark(_do)
    assert_results(res, expected, info_only=True)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "pathspecs", "expected"], **s3_data.pytest_benchmark_many_case()
)
@pytest.mark.benchmark(max_time=30)
def test_info_many_benchmark(
    benchmark, inject_failure_rate, s3root, pathspecs, expected
):
    urls = []
    check_expected = {}
    for count, v in expected:
        urls.extend(list(v) * count)
        if count > 0:
            check_expected.update(v)
    random.shuffle(urls)

    def _do():
        with S3(inject_failure_rate=inject_failure_rate) as s3:
            res = s3.info_many(urls)
        return res

    res = benchmark(_do)
    assert_results(res, check_expected, info_only=True)


@pytest.mark.parametrize(
    argnames=["s3root", "pathspecs", "expected"], **s3_data.pytest_benchmark_case()
)
@pytest.mark.benchmark(max_time=60)
def test_get_one_benchmark(benchmark, s3root, pathspecs, expected):
    def _do():
        with S3() as s3:
            res = []
            for url in expected:
                # Use return_missing as this is the most expensive path
                res.append(s3.get(url, return_missing=True))
            return res

    res = benchmark(_do)
    # We do not actually check results because the files will be cleared
    # Could be improved if we want to be real precise
    # assert_results(res, expected, info_should_be_empty=True)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "pathspecs", "expected"], **s3_data.pytest_benchmark_many_case()
)
@pytest.mark.benchmark(max_time=60)
def test_get_many_benchmark(
    benchmark, inject_failure_rate, s3root, pathspecs, expected
):
    urls = []
    check_expected = {}
    for count, v in expected:
        urls.extend(list(v) * count)
        if count > 0:
            check_expected.update(v)
    random.shuffle(urls)

    def _do():
        with S3(inject_failure_rate=inject_failure_rate) as s3:
            # Use return_missing as this is the most expensive path
            res = s3.get_many(urls, return_missing=True)
        return res

    res = benchmark(_do)
    # assert_results(res, check_expected, info_should_be_empty=True)


@pytest.mark.parametrize(
    argnames=["s3root", "blobs", "expected"], **s3_data.pytest_benchmark_put_case()
)
@pytest.mark.benchmark(max_time=60)
def test_put_one_benchmark(benchmark, tempdir, s3root, blobs, expected):
    # We generate the files here to avoid having them saved in the benchmark
    # result file which then prevents comparisons
    def _generate_files(blobs):
        for blob in blobs:
            prefix, fname, size = blob
            data = s3_data.RandomFile(prefix, fname, size)
            key = str(uuid4())
            path = os.path.join(tempdir, key)
            with open(path, "wb") as f:
                f.write(data.data)
            yield key, path

    # Generate all files before the test so that we don't time this
    all_files = list(_generate_files(blobs))

    def _do():
        with S3(s3root=s3root) as s3:
            res = []
            for key, obj in all_files:
                key = str(uuid4())  # New "name" every time
                res.append(s3.put(key, obj, overwrite=False))
            return res

    res = benchmark(_do)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "blobs", "expected"], **s3_data.pytest_benchmark_put_many_case()
)
@pytest.mark.benchmark(max_time=60)
def test_put_many_benchmark(
    benchmark, tempdir, inject_failure_rate, s3root, blobs, expected
):
    def _generate_files(blobs):
        generated_paths = {}
        for blob in blobs:
            count, blob_info = blob
            if blob_info in generated_paths:
                for _ in range(count):
                    yield str(uuid4()), generated_paths[blob_info]
            else:
                prefix, fname, size = blob_info
                data = s3_data.RandomFile(prefix, fname, size)
                key = str(uuid4())
                path = os.path.join(tempdir, key)
                with open(path, "wb") as f:
                    f.write(data.data)
                generated_paths[blob_info] = path
                for _ in range(count):
                    yield str(uuid4()), path

    all_files = list(_generate_files(blobs))

    def _do():
        new_files = [(str(uuid4()), path) for _, path in all_files]
        with S3(s3root=s3root, inject_failure_rate=inject_failure_rate) as s3:
            s3urls = s3.put_files(new_files, overwrite=False)
        return s3urls

    res = benchmark(_do)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "pathspecs", "expected"], **s3_data.pytest_fakerun_cases()
)
def test_init_options(inject_failure_rate, s3root, pathspecs, expected):
    [pathspec] = pathspecs
    flow_name, run_id = pathspec.split("/")
    plen = len(s3root)

    # option 1) s3root as prefix
    with S3(s3root=s3root) as s3:
        for url, exp in expected.items():
            # s3root should work as a prefix
            s3obj = s3.get(url[plen:])
            assert s3obj.key == url[plen:]
            assert_results([s3obj], {url: exp})
        with pytest.raises(MetaflowS3URLException):
            s3.get("s3://some/fake/address")

    # option 2) full url as s3root
    for url, exp in expected.items():
        with S3(s3root=url) as s3:
            s3obj = s3.get()
            assert_results([s3obj], {url: exp})

    # option 3) full urls
    with S3(inject_failure_rate=inject_failure_rate) as s3:
        for url, exp in expected.items():
            # s3root should work as a prefix
            s3obj = s3.get(url)
            assert s3obj.key == url
            assert_results([s3obj], {url: exp})
        with pytest.raises(MetaflowS3URLException):
            s3.get("suffix")
        with pytest.raises(MetaflowS3URLException):
            s3.get("s3://nopath")
        with pytest.raises(MetaflowS3URLException):
            s3.get_many(["suffixes"])
        with pytest.raises(MetaflowS3URLException):
            s3.get_recursive(["suffixes"])
        with pytest.raises(MetaflowS3URLException):
            s3.get_all()

    # option 4) 'current' environment (fake a running flow)
    flow = FakeFlow(use_cli=False)
    parsed = urlparse(s3root)

    # Once current is set, we can't test again. It doesn't inject failures anyways so OK
    if inject_failure_rate == 0:
        with pytest.raises(MetaflowS3URLException):
            # current not set yet, so this should fail
            with S3(run=flow):
                pass

    current._set_env(
        FakeFlow(name=flow_name),
        run_id,
        "no_step",
        "no_task",
        "no_origin_run_id",
        "no_ns",
        "no_user",
    )

    with S3(
        bucket=parsed.netloc,
        prefix=parsed.path,
        run=flow,
        inject_failure_rate=inject_failure_rate,
    ) as s3:
        for url, exp in expected.items():
            name = url.split("/")[-1]
            s3obj = s3.get(name)
            assert s3obj.key == name
            assert_results([s3obj], {url: exp})
        names = [url.split("/")[-1] for url in expected]
        s3objs = s3.get_many(names)
        assert {e.key for e in s3objs} == set(names)
        assert_results(s3objs, expected)
        assert_results(s3.get_all(), expected, info_should_be_empty=True)

    # option 5) run object
    if DO_TEST_RUN:
        # Only works if a metadata service exists with the run in question.
        namespace(None)
        with S3(
            bucket=parsed.netloc,
            prefix=parsed.path,
            run=Run(pathspec),
            inject_failure_rate=inject_failure_rate,
        ) as s3:
            names = [url.split("/")[-1] for url in expected]
            assert_results(s3.get_many(names), expected)


@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_basic_case()
)
def test_info_one(s3root, prefixes, expected):
    with S3() as s3:
        for url, item in expected.items():
            if item[None].size is None:
                # ensure that the default return_missing=False works
                with pytest.raises(MetaflowS3NotFound):
                    s3obj = s3.info(url)
                # test return_missing=True
                s3obj = s3.info(url, return_missing=True)
                assert_results([s3obj], {url: expected[url]}, info_only=True)
            else:
                s3obj = s3.info(url)
                assert_results([s3obj], {url: expected[url]}, info_only=True)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_basic_case()
)
def test_info_many(inject_failure_rate, s3root, prefixes, expected):
    with S3(inject_failure_rate=inject_failure_rate) as s3:
        # 1) test the non-missing case

        # to test result ordering, make sure we are requesting
        # keys in a non-lexicographic order
        not_missing = [url for url, v in expected.items() if v[None].size is not None]
        urls = list(sorted(not_missing, reverse=True))
        s3objs = s3.info_many(urls)

        # results should come out in the order of keys requested
        assert urls == [e.url for e in s3objs]
        assert_results(s3objs, {k: expected[k] for k in not_missing}, info_only=True)

        # 2) test with missing items, default case
        if not_missing != list(expected):
            with pytest.raises(MetaflowS3NotFound):
                s3objs = s3.info_many(list(expected))

        # 3) test with missing items, return_missing=True

        # to test result ordering, make sure we are requesting
        # keys in a non-lexicographic order. Missing files should
        # be returned in order too
        urls = list(sorted(expected, reverse=True))
        s3objs = s3.info_many(urls, return_missing=True)
        assert urls == [e.url for e in s3objs]
        assert_results(s3objs, expected, info_only=True)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_fakerun_cases()
)
def test_get_exceptions(inject_failure_rate, s3root, prefixes, expected):
    # get_many() goes via s3op, get() is a method - test both the code paths
    with S3(inject_failure_rate=inject_failure_rate) as s3:
        with pytest.raises(MetaflowS3AccessDenied):
            s3.get_many(["s3://foobar/foo"])
        with pytest.raises(MetaflowS3AccessDenied):
            s3.get("s3://foobar/foo")
    with S3(s3root=s3root) as s3:
        with pytest.raises(MetaflowS3NotFound):
            s3.get_many(["this_file_does_not_exist"])
        with pytest.raises(MetaflowS3NotFound):
            s3.get("this_file_does_not_exist")


@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_basic_case()
)
def test_get_one(s3root, prefixes, expected):
    with S3() as s3:
        for url, item in expected.items():
            for _, expected_result in item.items():
                range_info = expected_result.range
                if expected_result.size is None:
                    # ensure that the default return_missing=False works
                    with pytest.raises(MetaflowS3NotFound):
                        s3obj = s3.get(s3_get_object_from_url_range(url, range_info))
                    # test return_missing=True
                    s3obj = s3.get(
                        s3_get_object_from_url_range(url, range_info),
                        return_missing=True,
                    )
                    assert_results([s3obj], {url: item}, ranges_fetched=[range_info])
                else:
                    s3obj = s3.get(
                        s3_get_object_from_url_range(url, range_info), return_info=True
                    )
                    assert_results([s3obj], {url: item}, ranges_fetched=[range_info])


@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_basic_case()
)
def test_get_one_wo_meta(s3root, prefixes, expected):
    with S3() as s3:
        for url, item in expected.items():
            for _, expected_result in item.items():
                range_info = expected_result.range
                if expected_result.size is None:
                    # ensure that the default return_missing=False works
                    with pytest.raises(MetaflowS3NotFound):
                        s3obj = s3.get(s3_get_object_from_url_range(url, range_info))
                    s3obj = s3.get(
                        s3_get_object_from_url_range(url, range_info),
                        return_missing=True,
                        return_info=False,
                    )
                    assert_results(
                        [s3obj],
                        {url: item},
                        info_should_be_empty=True,
                        ranges_fetched=[range_info],
                    )
                else:
                    s3obj = s3.get(
                        s3_get_object_from_url_range(url, range_info), return_info=False
                    )
                    assert_results(
                        [s3obj],
                        {url: item},
                        info_should_be_empty=True,
                        ranges_fetched=[range_info],
                    )


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_large_case()
)
def test_get_all(inject_failure_rate, s3root, prefixes, expected):
    expected_exists = {
        url: v for url, v in expected.items() if v[None].size is not None
    }
    for prefix in prefixes:
        with S3(
            s3root=os.path.join(s3root, prefix), inject_failure_rate=inject_failure_rate
        ) as s3:
            s3objs = s3.get_all()
            # results should be in lexicographic order
            assert list(sorted(e.url for e in s3objs)) == [e.url for e in s3objs]
            assert_results(s3objs, expected_exists, info_should_be_empty=True)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_basic_case()
)
def test_get_all_with_meta(inject_failure_rate, s3root, prefixes, expected):
    expected_exists = {
        url: v for url, v in expected.items() if v[None].size is not None
    }
    for prefix in prefixes:
        with S3(
            s3root=os.path.join(s3root, prefix), inject_failure_rate=inject_failure_rate
        ) as s3:
            s3objs = s3.get_all(return_info=True)
            # results should be in lexicographic order
            assert list(sorted(e.url for e in s3objs)) == [e.url for e in s3objs]
            assert_results(s3objs, expected_exists)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_basic_case()
)
def test_get_many(inject_failure_rate, s3root, prefixes, expected):
    def iter_objs(urls, objs):
        for url in urls:
            obj = objs[url]
            for r, expected in obj.items():
                if r is None:
                    yield url, None, None
                else:
                    yield url, expected.range.req_offset, expected.range.req_size

    with S3(inject_failure_rate=inject_failure_rate) as s3:
        # 1) test the non-missing case

        # to test result ordering, make sure we are requesting
        # keys in a non-lexicographic order
        not_missing_urls = [k for k, v in expected.items() if v[None].size is not None]
        urls_in_order = list(sorted(not_missing_urls, reverse=True))
        ranges_in_order = []
        for url in urls_in_order:
            ranges_in_order.extend(v.range for v in expected[url].values())

        objs_in_order = list(starmap(S3GetObject, iter_objs(urls_in_order, expected)))
        s3objs = s3.get_many(list(objs_in_order), return_info=True)

        fetched_urls = []
        for url in urls_in_order:
            fetched_urls.extend([url] * len(expected[url]))
        # results should come out in the order of keys requested
        assert fetched_urls == [e.url for e in s3objs]
        assert_results(s3objs, expected, ranges_fetched=ranges_in_order)

        # 2) test with missing items, default case
        if not_missing_urls != list(expected.keys()):
            urls_in_order = list(sorted(expected.keys(), reverse=True))
            ranges_in_order = []
            for url in urls_in_order:
                ranges_in_order.extend(v.range for v in expected[url].values())
            objs_in_order = list(
                starmap(S3GetObject, iter_objs(urls_in_order, expected))
            )
            fetched_urls = []
            for url in urls_in_order:
                fetched_urls.extend([url] * len(expected[url]))
            with pytest.raises(MetaflowS3NotFound):
                s3objs = s3.get_many(list(objs_in_order), return_info=True)

        # 3) test with missing items, return_missing=True

        # to test result ordering, make sure we are requesting
        # keys in a non-lexicographic order. Missing files should
        # be returned in order too
        # Here we can use urls_in_order, ranges_in_order and objs_in_order because they
        # always correspond to the full set
        s3objs = s3.get_many(list(objs_in_order), return_missing=True, return_info=True)
        assert fetched_urls == [e.url for e in s3objs]
        assert_results(s3objs, expected, ranges_fetched=ranges_in_order)


@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_many_prefixes_case()
)
def test_list_paths(s3root, prefixes, expected):
    def urls_by_prefix(prefix):
        root = os.path.join(s3root, prefix)
        for url, v in expected.items():
            if url.startswith(root) and v[None].size is not None:
                yield url

    # 1) test that list_paths() without arguments works
    matches = {prefix: frozenset(urls_by_prefix(prefix)) for prefix in prefixes}
    non_empty = {prefix for prefix, urls in matches.items() if urls}

    with S3(s3root=s3root) as s3:
        s3objs = s3.list_paths()
        # found_prefixes is a subset of paths under s3root
        found_prefixes = [e for e in s3objs if e.key in prefixes]
        # we expect to find all non-empty prefixes under the s3root
        assert {e.key for e in found_prefixes} == non_empty
        # they should be all marked as non-existent objects, just prefixes
        assert all(not e.exists for e in found_prefixes)
        # they should be all marked as not downloaded
        assert all(not e.downloaded for e in found_prefixes)

    # 2) test querying by many prefixes
    with S3(s3root=s3root) as s3:
        s3objs = s3.list_paths(prefixes)
        assert (
            frozenset(e.prefix.rstrip("/").split("/")[-1] for e in s3objs) == non_empty
        )

        for prefix, exp in matches.items():
            exists = frozenset(e.url for e in s3objs if e.prefix == prefix and e.exists)
            not_exists = frozenset(
                e.url for e in s3objs if e.prefix == prefix and not e.exists
            )
            # every object should be expected
            assert all(e in exp for e in exists)
            # not existing ones are prefixes, they shouldn't match
            assert all(e not in exp for e in not_exists)

    # 3) eventually list_paths should hit the leaf
    for url, v in expected.items():
        if v[None].size is None:
            with S3() as s3:
                # querying a non-existent object should return
                # prefixes or nothing
                s3objs = s3.list_paths([url])
                assert [e for e in s3objs if e.exists] == []
        else:
            suffix = url[len(s3root) :]
            expected_keys = suffix.split("/")
            if len(expected_keys) > 20:
                # speed optimization: exclude crazy long paths
                continue
            got_url = s3root
            for idx, expected_key in enumerate(expected_keys):
                with S3(s3root=got_url) as s3:
                    s3objs = s3.list_paths()
                    # are we at the leaf?
                    if idx == len(expected_keys) - 1:
                        # a leaf object should always exist
                        [match] = [
                            e for e in s3objs if e.key == expected_key and e.exists
                        ]
                    else:
                        # a non-leaf may match objects that are also prefixes
                        [match] = [
                            e for e in s3objs if e.key == expected_key and not e.exists
                        ]
                    # prefix + key == url
                    assert os.path.join(match.prefix, match.key) == match.url.rstrip(
                        "/"
                    )
                    got_url = match.url

            # the leaf should be the object itself
            assert match.url == url


@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_many_prefixes_case()
)
def test_list_recursive(s3root, prefixes, expected):
    not_missing = [url for url, v in expected.items() if v[None].size is not None]
    with S3(s3root=s3root) as s3:
        s3objs = s3.list_recursive(prefixes)
        assert frozenset(e.url for e in s3objs) == frozenset(not_missing)
        # ensure that there are no duplicates
        assert len(s3objs) == len(not_missing)
        # list_recursive returns leaves only
        assert all(e.exists for e in s3objs)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "prefixes", "expected"], **s3_data.pytest_many_prefixes_case()
)
def test_get_recursive(inject_failure_rate, s3root, prefixes, expected):
    expected_exists = {
        url: v for url, v in expected.items() if v[None].size is not None
    }
    local_files = []
    with S3(s3root=s3root, inject_failure_rate=inject_failure_rate) as s3:
        s3objs = s3.get_recursive(prefixes)

        # we need to deduce which prefixes actually produce results
        nonempty_prefixes = list(
            filter(
                lambda p: any(
                    url.startswith(os.path.join(s3root, p)) for url in expected_exists
                ),
                prefixes,
            )
        )

        # prefixes must be returned in the order of prefixes requested
        plen = len(s3root)
        grouped = list(groupby(s3objs, lambda e: e.prefix[plen:]))

        assert nonempty_prefixes == [prefix for prefix, _ in grouped]
        # for each prefix, the results should be in lexicographic order
        for prefix, objs in grouped:
            urls = [e.url for e in objs]
            assert list(sorted(urls)) == urls

        assert_results(s3objs, expected_exists, info_should_be_empty=True)

        # if there are multiple prefixes, it is a bit harder to know
        # what's the expected set of results. We do this test only
        # for the single-prefix case for now
        if len(prefixes) == 1:
            [prefix] = prefixes
            s3root = os.path.join(s3root, prefix)
            keys = {url[len(s3root) + 1 :] for url in expected_exists}
            assert {e.key for e in s3objs} == keys

        local_files = [s3obj.path for s3obj in s3objs]
    # local files must not exist outside the S3 context
    for path in local_files:
        assert not os.path.exists(path)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
def test_put_exceptions(inject_failure_rate):
    with S3(inject_failure_rate=inject_failure_rate) as s3:
        with pytest.raises(MetaflowS3InvalidObject):
            s3.put_many([("a", 1)])
        with pytest.raises(MetaflowS3InvalidObject):
            s3.put("a", 1)
        with pytest.raises(MetaflowS3NotFound):
            s3.put_files([("a", "/non-existent/local-file")])
        with pytest.raises(MetaflowS3URLException):
            s3.put_many([("foo", "bar")])


@pytest.fixture
def s3_server_side_encryption():
    return "AES256"


@pytest.mark.parametrize("inject_failure_rate", [0])
@pytest.mark.parametrize(
    argnames=["s3root", "objs", "expected"], **s3_data.pytest_put_strings_case()
)
def test_put_many(
    inject_failure_rate, s3root, objs, expected, s3_server_side_encryption
):
    encryption_settings = [None, s3_server_side_encryption]
    for setting in encryption_settings:
        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            s3urls = s3.put_many(objs)
            assert list(dict(s3urls)) == list(dict(objs))
            # results must be in the same order as the keys requested
            for i in range(len(s3urls)):
                assert objs[i][0] == s3urls[i][0]
        with S3(inject_failure_rate=inject_failure_rate, encryption=setting) as s3:
            s3objs = s3.get_many(dict(s3urls).values())
            assert_results(s3objs, expected)
        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            s3objs = s3.get_many(list(dict(objs)))
            assert {s3obj.key for s3obj in s3objs} == {key for key, _ in objs}

        # upload shuffled objs with overwrite disabled
        shuffled_objs = deranged_shuffle(objs)
        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            overwrite_disabled_s3urls = s3.put_many(shuffled_objs, overwrite=False)
            assert len(overwrite_disabled_s3urls) == 0
        with S3(inject_failure_rate=inject_failure_rate, encryption=setting) as s3:
            s3objs = s3.get_many(dict(s3urls).values())
            assert_results(s3objs, expected)


@pytest.mark.parametrize(
    argnames=["s3root", "objs", "expected"], **s3_data.pytest_put_strings_case()
)
def test_put_one(s3root, objs, expected, s3_server_side_encryption):
    encryption_settings = [None, s3_server_side_encryption]
    for setting in encryption_settings:
        with S3(s3root=s3root, encryption=setting) as s3:
            for key, obj in objs:
                s3url = s3.put(key, obj)
                assert s3url in expected
                s3obj = s3.get(key)
                assert s3obj.key == key
                assert_results([s3obj], {s3url: expected[s3url]}, encryption=setting)
                assert s3obj.blob == to_bytes(obj)
                # put with overwrite disabled
                s3url = s3.put(key, "random_value", overwrite=False)
                assert s3url in expected
                s3obj = s3.get(key)
                assert s3obj.key == key
                assert_results([s3obj], {s3url: expected[s3url]}, encryption=setting)
                assert s3obj.blob == to_bytes(obj)


@pytest.mark.parametrize("inject_failure_rate", [0, 10, 50, 90])
@pytest.mark.parametrize(
    argnames=["s3root", "blobs", "expected"], **s3_data.pytest_put_blobs_case()
)
def test_put_files(
    tempdir, inject_failure_rate, s3root, blobs, expected, s3_server_side_encryption
):
    def _files(blobs):
        for blob in blobs:
            key = getattr(blob, "key", blob[0])
            data = getattr(blob, "value", blob[1])
            content_type = getattr(blob, "content_type", None)
            metadata = getattr(blob, "metadata", None)
            encryption = getattr(blob, "encryption", None)
            path = os.path.join(tempdir, key)
            with open(path, "wb") as f:
                f.write(data)
            yield S3PutObject(
                key=key,
                path=path,
                content_type=content_type,
                metadata=metadata,
                encryption=encryption,
            )

    encryption_settings = [None, s3_server_side_encryption]
    for setting in encryption_settings:
        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            s3urls = s3.put_files(_files(blobs))
            assert list(dict(s3urls)) == list(dict(blobs))

        with S3(inject_failure_rate=inject_failure_rate, encryption=setting) as s3:
            # get urls
            s3objs = s3.get_many(dict(s3urls).values())
            assert_results(s3objs, expected, encryption=setting)

        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            # get keys
            s3objs = s3.get_many(key for key, blob in blobs)
            assert {s3obj.key for s3obj in s3objs} == {key for key, _ in blobs}

        # upload shuffled blobs with overwrite disabled
        shuffled_blobs = blobs[:]
        shuffle(shuffled_blobs)
        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            overwrite_disabled_s3urls = s3.put_files(
                _files(shuffled_blobs), overwrite=False
            )
            assert len(overwrite_disabled_s3urls) == 0

        with S3(inject_failure_rate=inject_failure_rate, encryption=setting) as s3:
            s3objs = s3.get_many(dict(s3urls).values())
            assert_results(s3objs, expected, encryption=setting)
        with S3(
            s3root=s3root, inject_failure_rate=inject_failure_rate, encryption=setting
        ) as s3:
            s3objs = s3.get_many(key for key, blob in shuffled_blobs)
            assert {s3obj.key for s3obj in s3objs} == {key for key, _ in shuffled_blobs}
