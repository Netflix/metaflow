from metaflow import FlowSpec, step, S3, current, project
import json


@project(name="hello_s3")
class HelloS3Flow(FlowSpec):
    @step
    def start(self):
        # Write a known payload to S3 and read it back
        test_data = {"hello": "s3", "run_id": current.run_id}
        payload = json.dumps(test_data).encode("utf-8")

        with S3(run=self) as s3:
            url = s3.put("test_object.json", payload)
            obj = s3.get("test_object.json")
            result = json.loads(obj.blob)

        assert result == test_data, f"S3 round-trip failed: {result} != {test_data}"
        self.s3_verified = True
        self.next(self.end)

    @step
    def end(self):
        assert self.s3_verified, "S3 verification did not complete"


if __name__ == "__main__":
    HelloS3Flow()
