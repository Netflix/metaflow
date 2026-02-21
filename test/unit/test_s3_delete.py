"""
Unit tests for S3 delete APIs (delete, delete_many, delete_recursive).

Tests use moto to mock S3 without requiring real AWS credentials or services.
"""

import pytest
from moto import mock_aws
import boto3
from metaflow.plugins.datatools.s3.s3 import S3, S3Client


@mock_aws
def test_s3_delete_single_object():
    """Test S3.delete() deletes a single object."""
    # Setup: Create a bucket and put an object
    s3_res = boto3.resource("s3", region_name="us-east-1")
    bucket = s3_res.create_bucket(Bucket="test-bucket")
    s3_res.Object("test-bucket", "test-key").put(Body=b"test data")

    # Initialize S3 client with explicit bucket
    s3_client = S3(s3root="s3://test-bucket")

    # Verify object exists before delete
    assert s3_client.info("test-key").exists

    # Delete the object
    s3_client.delete("test-key")

    # Verify object is gone
    assert not s3_client.info("test-key", return_missing=True).exists

    s3_client.close()


@mock_aws
def test_s3_delete_many_objects():
    """Test S3.delete_many() deletes multiple objects in batches."""
    # Setup
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_res.create_bucket(Bucket="test-bucket")

    # Create 5 test objects
    keys = [f"obj-{i}" for i in range(5)]
    for key in keys:
        s3_res.Object("test-bucket", key).put(Body=b"data")

    # Initialize S3 client
    s3_client = S3(s3root="s3://test-bucket")

    # Verify all objects exist
    for key in keys:
        assert s3_client.info(key).exists

    # Delete all objects at once
    s3_client.delete_many(keys)

    # Verify all objects are deleted
    for key in keys:
        assert not s3_client.info(key, return_missing=True).exists

    s3_client.close()


@mock_aws
def test_s3_delete_many_large_batch():
    """Test S3.delete_many() handles >1000 keys (batching)."""
    # Setup
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_res.create_bucket(Bucket="test-bucket")

    # Create 1500 test objects
    num_keys = 1500
    keys = [f"obj-{i:04d}" for i in range(num_keys)]
    for key in keys:
        s3_res.Object("test-bucket", key).put(Body=b"data")

    # Initialize S3 client
    s3_client = S3(s3root="s3://test-bucket")

    # Verify objects exist
    for key in keys[:10]:  # sample check
        assert s3_client.info(key).exists

    # Delete all objects at once (should batch into 2 calls: 1000 + 500)
    s3_client.delete_many(keys)

    # Verify sampled objects are deleted
    for key in keys[:10]:
        assert not s3_client.info(key, return_missing=True).exists

    s3_client.close()


@mock_aws
def test_s3_delete_nonexistent_key():
    """Test S3.delete() handles nonexistent keys gracefully."""
    # Setup
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_res.create_bucket(Bucket="test-bucket")

    # Initialize S3 client
    s3_client = S3(s3root="s3://test-bucket")

    # Delete a key that doesn't exist (should not raise)
    s3_client.delete("nonexistent-key")

    s3_client.close()


@mock_aws
def test_s3_delete_with_full_url():
    """Test S3.delete() works with full s3:// URLs."""
    # Setup
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_res.create_bucket(Bucket="test-bucket")
    s3_res.Object("test-bucket", "test-key").put(Body=b"test data")

    # Initialize S3 client without s3root (require full URLs)
    s3_client = S3()

    # Delete using full URL
    s3_client.delete("s3://test-bucket/test-key")

    # Verify object is deleted
    assert not s3_client.info("s3://test-bucket/test-key", return_missing=True).exists

    s3_client.close()


@mock_aws
def test_s3_delete_many_cross_bucket():
    """Test S3.delete_many() handles keys from multiple buckets."""
    # Setup
    s3_res = boto3.resource("s3", region_name="us-east-1")
    s3_res.create_bucket(Bucket="bucket1")
    s3_res.create_bucket(Bucket="bucket2")

    # Create objects in different buckets
    s3_res.Object("bucket1", "key1").put(Body=b"data")
    s3_res.Object("bucket2", "key2").put(Body=b"data")

    # Initialize S3 client (without s3root to use full URLs)
    s3_client = S3()

    # Delete objects from both buckets
    s3_client.delete_many(["s3://bucket1/key1", "s3://bucket2/key2"])

    # Verify both are deleted
    assert not s3_client.info("s3://bucket1/key1", return_missing=True).exists
    assert not s3_client.info("s3://bucket2/key2", return_missing=True).exists

    s3_client.close()
