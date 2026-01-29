"""
Test cases for the S3 double slash fix (Issue #684).

This module tests the _s3_path_join() function and verifies that S3 paths
are correctly constructed without double slashes when the s3root configuration
contains a trailing slash.

BEFORE FIX:
-----------
When METAFLOW_DATASTORE_SYSROOT_S3 = "s3://bucket/metaflow/" (with trailing slash)
Generated paths would contain double slashes: "s3://bucket/metaflow//flow/run"

AFTER FIX:
----------
With _s3_path_join(), paths are normalized: "s3://bucket/metaflow/flow/run"
"""

import os
import pytest
from metaflow import current
from metaflow.plugins.datatools.s3 import S3, _s3_path_join
from .. import FakeFlow, S3ROOT

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


class TestS3PathJoinFunction:
    """
    Test the _s3_path_join() helper function directly.

    This function was added to fix issue #684 where os.path.join() would
    create double slashes in S3 paths when the root had a trailing slash.
    """

    def test_basic_join(self):
        """Basic path joining with no edge cases."""
        result = _s3_path_join("s3://bucket/prefix", "folder", "file.txt")
        assert result == "s3://bucket/prefix/folder/file.txt"

    def test_join_with_trailing_slash_in_first_part(self):
        """First part has trailing slash - should strip it."""
        result = _s3_path_join("s3://bucket/prefix/", "folder", "file.txt")
        assert result == "s3://bucket/prefix/folder/file.txt"
        # No double slashes
        assert "//" not in result.replace("s3://", "")

    def test_join_with_leading_slash_in_middle_parts(self):
        """Middle parts have leading slashes - should strip them."""
        result = _s3_path_join("s3://bucket/prefix", "/folder", "/file.txt")
        assert result == "s3://bucket/prefix/folder/file.txt"
        # No double slashes
        assert "//" not in result.replace("s3://", "")

    def test_join_with_both_trailing_and_leading_slashes(self):
        """Both trailing and leading slashes - should normalize."""
        result = _s3_path_join("s3://bucket/prefix/", "/folder/", "/file.txt")
        assert result == "s3://bucket/prefix/folder/file.txt"
        # No double slashes
        assert "//" not in result.replace("s3://", "")

    def test_join_with_multiple_trailing_slashes(self):
        """Multiple trailing slashes should all be stripped."""
        result = _s3_path_join("s3://bucket/prefix///", "folder")
        assert result == "s3://bucket/prefix/folder"
        # No double slashes
        assert "//" not in result.replace("s3://", "")

    def test_join_empty_parts_filtered(self):
        """Empty parts should be filtered out."""
        result = _s3_path_join("s3://bucket", "", "folder", "", "file.txt")
        assert result == "s3://bucket/folder/file.txt"

    def test_join_single_part(self):
        """Single part should return as-is with trailing slash stripped."""
        result = _s3_path_join("s3://bucket/prefix/")
        assert result == "s3://bucket/prefix"

    def test_join_preserves_s3_protocol(self):
        """S3 protocol prefix should be preserved."""
        result = _s3_path_join("s3://bucket", "folder")
        assert result.startswith("s3://")
        # Only one "//" should be the protocol
        assert result.count("//") == 1


class TestS3InitWithTrailingSlash:
    """
    Test S3 initialization with trailing slash in configuration.

    These tests verify the fix for issue #684 where trailing slashes in
    METAFLOW_DATASTORE_SYSROOT_S3 would cause double slashes in generated paths.
    """

    def test_s3root_with_trailing_slash_creates_valid_paths(self, s3root):
        """
        Verify that s3root with trailing slash doesn't create double slashes.

        BEFORE FIX: s3root="s3://bucket/path/" + key="file" = "s3://bucket/path//file"
        AFTER FIX:  s3root="s3://bucket/path/" + key="file" = "s3://bucket/path/file"
        """
        # Ensure we're testing with a trailing slash
        s3root_with_slash = s3root if s3root.endswith("/") else s3root + "/"

        with S3(s3root=s3root_with_slash) as s3:
            # Test _url method with various keys
            test_key = "testfile.txt"
            url = s3._url(test_key)

            # Should not have double slashes (except in s3:// protocol)
            path_without_protocol = url.replace("s3://", "")
            assert "//" not in path_without_protocol, f"Found double slash in path: {url}"

            # Should end with the key
            assert url.endswith(test_key)

    def test_s3root_without_trailing_slash_creates_valid_paths(self, s3root):
        """
        Verify that s3root without trailing slash still works correctly.

        This ensures the fix doesn't break the non-trailing-slash case.
        """
        # Ensure we're testing without a trailing slash
        s3root_no_slash = s3root.rstrip("/")

        with S3(s3root=s3root_no_slash) as s3:
            test_key = "testfile.txt"
            url = s3._url(test_key)

            # Should not have double slashes (except in s3:// protocol)
            path_without_protocol = url.replace("s3://", "")
            assert "//" not in path_without_protocol, f"Found double slash in path: {url}"

            # Should end with the key
            assert url.endswith(test_key)

    def test_s3root_comparison_with_and_without_slash(self, s3root):
        """
        Both configurations should produce identical final URLs.

        This is the key test: trailing slash should not affect the final path.
        """
        s3root_with_slash = s3root if s3root.endswith("/") else s3root + "/"
        s3root_no_slash = s3root.rstrip("/")

        test_key = "folder/testfile.txt"

        with S3(s3root=s3root_with_slash) as s3_with:
            url_with_slash = s3_with._url(test_key)

        with S3(s3root=s3root_no_slash) as s3_without:
            url_without_slash = s3_without._url(test_key)

        # Both should produce the same URL
        assert url_with_slash == url_without_slash, (
            f"URLs differ:\n"
            f"  With trailing slash:    {url_with_slash}\n"
            f"  Without trailing slash: {url_without_slash}"
        )

        # Neither should have double slashes
        assert "//" not in url_with_slash.replace("s3://", "")
        assert "//" not in url_without_slash.replace("s3://", "")


class TestS3InitWithRunAndTrailingSlash:
    """
    Test S3 initialization with run parameter and trailing slash.

    This tests the fix in S3.__init__() where bucket and prefix are joined
    when initializing with a run parameter.
    """

    def test_run_initialization_with_trailing_slash(self, s3root):
        """
        Test that run initialization handles trailing slashes correctly.

        This tests the specific code path in S3.__init__() that was affected:
        self._s3root = "s3://%s" % os.path.join(bucket, prefix.strip("/"))

        Now uses _s3_path_join() to avoid double slashes.
        """
        parsed = urlparse(s3root)
        bucket = parsed.netloc

        # Test with various prefix configurations
        prefixes = [
            "metaflow/",       # trailing slash
            "metaflow",        # no trailing slash
            "/metaflow/",      # both leading and trailing
            "/metaflow",       # only leading
        ]

        for prefix in prefixes:
            flow = FakeFlow(use_cli=False, name="TestFlow")
            run_id = "12345"

            # Set up the current environment
            current._set_env(
                flow,
                run_id,
                "step",
                "task",
                "origin_run_id",
                "namespace",
                "user",
            )

            # Initialize S3 with run parameter
            with S3(bucket=bucket, prefix=prefix, run=flow) as s3:
                # The internal _s3root should not have double slashes
                s3root_path = s3._s3root.replace("s3://", "")
                assert "//" not in s3root_path, (
                    f"Found double slash in s3root with prefix '{prefix}': {s3._s3root}"
                )

                # Should contain the flow name and run ID
                assert "TestFlow" in s3._s3root
                assert run_id in s3._s3root


class TestS3PathEdgeCases:
    """
    Test edge cases for S3 path construction.
    """

    def test_key_with_leading_slash(self, s3root):
        """
        Keys with leading slashes should be handled correctly.

        The original code had this comment:
        # Strip leading slashes to ensure os.path.join works correctly
        # os.path.join discards the first argument if the second starts with '/'

        With _s3_path_join(), leading slashes are always stripped.
        """
        s3root_with_slash = s3root if s3root.endswith("/") else s3root + "/"

        with S3(s3root=s3root_with_slash) as s3:
            # Test key with leading slash
            url = s3._url("/file.txt")

            # Should not have double slashes
            assert "//" not in url.replace("s3://", "")

            # Should end with file.txt (not /file.txt)
            assert url.endswith("/file.txt") or url.endswith("file.txt")

    def test_nested_path_construction(self, s3root):
        """
        Test deeply nested paths are constructed correctly.
        """
        s3root_with_slash = s3root if s3root.endswith("/") else s3root + "/"

        with S3(s3root=s3root_with_slash) as s3:
            nested_key = "a/b/c/d/e/file.txt"
            url = s3._url(nested_key)

            # Should not have double slashes anywhere
            path_without_protocol = url.replace("s3://", "")
            assert "//" not in path_without_protocol

            # Should preserve the nested structure
            assert nested_key in url


class TestBackwardCompatibility:
    """
    Test that the fix maintains backward compatibility.

    These tests ensure that existing code using S3 without trailing slashes
    continues to work exactly as before.
    """

    def test_existing_code_without_trailing_slash_unchanged(self, s3root):
        """
        Code using s3root without trailing slash should work identically.
        """
        s3root_no_slash = s3root.rstrip("/")

        with S3(s3root=s3root_no_slash) as s3:
            test_cases = [
                "file.txt",
                "folder/file.txt",
                "deep/nested/path/file.txt",
            ]

            for key in test_cases:
                url = s3._url(key)

                # Should construct valid paths
                assert url.startswith("s3://")
                assert url.endswith(key)
                assert "//" not in url.replace("s3://", "")

    def test_absolute_url_handling_unchanged(self, s3root):
        """
        Absolute S3 URLs should still work as before (raise exception when prefix is set).

        The fix should not affect this behavior.
        """
        from metaflow.plugins.datatools.s3 import MetaflowS3URLException

        with S3(s3root=s3root) as s3:
            # Absolute URLs should raise exception when s3root is set
            with pytest.raises(MetaflowS3URLException):
                s3._url("s3://other-bucket/file.txt")


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
