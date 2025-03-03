from metaflow.plugins.datatools.s3.s3op import convert_to_client_error


def test_convert_to_client_error():
    s = "boto3.exceptions.S3UploadFailedError: Failed to upload /a/b/c/d.parquet to e/f/g/h.parquet: An error occurred (SlowDown) when calling the CompleteMultipartUpload operation (reached max retries: 4): Please reduce your request rate."
    client_error = convert_to_client_error(s)
    assert client_error.response["Error"]["Code"] == "SlowDown"
    assert (
        client_error.response["Error"]["Message"] == "Please reduce your request rate."
    )
    assert client_error.operation_name == "CompleteMultipartUpload"
