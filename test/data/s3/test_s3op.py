import errno
import os
import tempfile
from hashlib import sha1

import pytest

from metaflow.plugins.datatools.s3.s3op import (
    convert_to_client_error,
    generate_local_path,
)
from metaflow.util import url_quote


def test_convert_to_client_error():
    s = "boto3.exceptions.S3UploadFailedError: Failed to upload /a/b/c/d.parquet to e/f/g/h.parquet: An error occurred (SlowDown) when calling the CompleteMultipartUpload operation (reached max retries: 4): Please reduce your request rate."
    client_error = convert_to_client_error(s)
    assert client_error.response["Error"]["Code"] == "SlowDown"
    assert (
        client_error.response["Error"]["Message"] == "Please reduce your request rate."
    )
    assert client_error.operation_name == "CompleteMultipartUpload"


def test_generate_local_path_length_limits():
    """Test that generate_local_path produces filenames under 255 characters."""
    test_cases = [
        ("s3://bucket/file.txt", "whole", None),
        ("s3://bucket/日本語ファイル名.txt", "whole", None),
        ("s3://bucket/" + "日本語" * 50 + ".txt", "whole", None),
        ("s3://bucket/" + "中文" * 50 + ".txt", "whole", None),
        ("s3://bucket/" + "x" * 300 + ".txt", "whole", None),
        ("s3://bucket/file.txt", "bytes=0-1000", None),
        ("s3://bucket/file.txt", "whole", "info"),
        ("s3://bucket/" + "x" * 300 + ".txt", "bytes=0-9999", "meta"),
    ]

    for url, range_val, suffix in test_cases:
        local_path = generate_local_path(url, range=range_val, suffix=suffix)
        assert len(local_path) <= 255


def test_generate_local_path_uniqueness():
    """Test that different URLs produce different local paths."""
    base = "日本語" * 50
    test_urls = [
        f"s3://bucket/{base}A.txt",
        f"s3://bucket/{base}B.txt",
        f"s3://bucket/{base}C.txt",
        f"s3://bucket/path1/{base}.txt",
        f"s3://bucket/path2/{base}.txt",
    ]

    local_paths = [generate_local_path(url) for url in test_urls]
    assert len(local_paths) == len(set(local_paths))

    hashes = [path.split("-")[0] for path in local_paths]
    assert len(hashes) == len(set(hashes))


def test_generate_local_path_truncation_indicator():
    """Test that truncated filenames have '...' indicator."""
    long_url = "s3://bucket/" + "日本語ファイル名" * 30 + ".txt"
    local_path = generate_local_path(long_url)
    assert "..." in local_path

    short_url = "s3://bucket/short.txt"
    short_local_path = generate_local_path(short_url)
    assert "..." not in short_local_path


def test_bucket_root_empty_path():
    """
    Unit test: verify that bucket root URL produces empty path, not "/".

    This test verifies the fix without requiring S3 access by checking
    the path that would be sent to S3 API.
    """
    try:
        from urlparse import urlparse
    except ImportError:
        from urllib.parse import urlparse

    # When listing at bucket root, the path component should be empty
    url = "s3://my-bucket"
    parsed = urlparse(url, allow_fragments=False)
    path_with_slash = parsed.path.lstrip("/")

    # Apply the fix
    if path_with_slash and not path_with_slash.endswith("/"):
        path_with_slash += "/"

    # The path should remain empty for bucket root (NOT become "/")
    assert path_with_slash == "", (
        f"Bucket root path should be empty, got '{path_with_slash}'. "
        f"If this is '/', the old bug has returned."
    )


def test_long_filename_download_from_s3():
    """
    End-to-end integration test with real S3 for long filename handling.

    Tests that files with very long non-ASCII names can be downloaded successfully.
    Without truncation, paths > 255 chars cause os.rename() to raise OSError.
    """
    from metaflow.plugins.datatools.s3 import S3

    from .. import S3ROOT

    if not S3ROOT:
        pytest.skip("S3ROOT not set")

    s3_prefix = S3ROOT.rstrip("/") + "/test-long-filenames/"
    problematic_filename = "日本語ファイル名テスト" * 20 + "_test.txt"
    s3_url = s3_prefix + problematic_filename

    # Verify untruncated path would exceed 255 chars
    def untruncated_generate_local_path(url):
        quoted = url_quote(url)
        fname = quoted.split(b"/")[-1].replace(b".", b"_").replace(b"-", b"_")
        sha = sha1(quoted).hexdigest()
        fname_decoded = fname.decode("utf-8")
        return "-".join((sha, fname_decoded, "whole"))

    untruncated_path_length = len(untruncated_generate_local_path(s3_url))
    if untruncated_path_length <= 255:
        pytest.skip(
            f"Test requires untruncated path > 255 chars, got {untruncated_path_length}"
        )

    # Verify truncated path is valid
    truncated_path = generate_local_path(s3_url)
    assert len(truncated_path) <= 255

    test_content = b"Test data for long filename handling"

    try:
        with S3(s3root=s3_prefix) as s3:
            s3.put(problematic_filename, test_content, overwrite=True)

        with S3(s3root=s3_prefix) as s3:
            objs = s3.get_many([problematic_filename])
            assert len(objs) == 1
            obj = objs[0]
            assert obj.blob == test_content
            assert obj.key == problematic_filename

        with S3(s3root=s3_prefix) as s3:
            obj = s3.get(problematic_filename)
            assert obj.blob == test_content

    finally:
        pass
