import time
from metaflow import FlowSpec, Parameter, current
from metaflow import S3


class BaseS3Flow(FlowSpec):
    bucket = Parameter("bucket", help="S3 bucket to watch")
    key = Parameter("key", default="", help="S3 key to check (file or prefix)")
    check_mode = Parameter(
        "check_mode",
        default="files_metadata",
        help="Mode to check: one of ['files_metadata', 'file_modified_ts', 'file_size']",
    )
    role = Parameter("role", help="IAM role for S3 access")

    @property
    def sensor_value(self):
        try:
            return current.trigger.run.data.value
        except:
            return None

    def _check_s3_based_file_metadata(self):
        with S3(role=self.role, s3root=self.bucket) as s3:
            keys = None if self.key in ("", "/") else [self.key]
            objects = s3.list_paths(keys)
            obj_metadata = [
                {
                    "key": obj.key,
                    "last_modified": obj.last_modified,
                    "size": obj.size,
                }
                for obj in objects
            ]
            return obj_metadata

    def _check_s3_based_file_modified_ts(self):
        with S3(role=self.role, s3root=self.bucket) as s3:
            s3obj = s3.get(self.key)
            return s3obj.last_modified if s3obj.exists else None

    def _check_s3_based_file_size(self):
        with S3(role=self.role, s3root=self.bucket) as s3:
            s3obj = s3.get(self.key)
            return s3obj.size if s3obj.exists else None

    def _query_s3(self):
        if self.check_mode == "files_metadata":
            return self._check_s3_based_file_metadata()

        elif self.check_mode == "file_modified_ts":
            return self._check_s3_based_file_modified_ts()

        elif self.check_mode == "file_size":
            return self._check_s3_based_file_size()
        else:
            raise ValueError(
                f"Unsupported check_mode '{self.check_mode}'. "
                f"Choose one of ['files_metadata', 'file_modified_ts', 'file_size']"
            )

    def query(self, storage_type="s3", **kwargs):
        if storage_type == "s3":
            return self._query_s3()
        else:
            raise ValueError(
                f"Invalid storage_type '{storage_type}'. Only 's3' is supported."
            )
