# Integration Tests for Metaflow on AIP

These integration tests are based on Metaflow flows that test edge cases (nested foreaches, complex branching, merging artifacts, etc). They can be run from the user's terminal or on a mirrored Gitlab repo through a push to this Github repo.

# Running Tests from the Terminal

When a user runs the tests from the terminal, the testing script launches test flows onto the Kubeflow cluster the user has access to and waits for completion.

First, create your Metaflow config file locally, which can be found at: `/Users/[your_username]/.metaflowconfig/config.json`.
This can also be obtained by running `metaflow configure show`. 

A sample configuration:
```
{
    "ARGO_RUN_URL_PREFIX": "https://argo-server.int.sandbox-k8s.zg-aip.net/",
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://serve-datalake-zillowgroup/zillow/workflow_sdk/metaflow_28d/dev/aip-playground",
    "METAFLOW_KUBERNETES_NAMESPACE": "aip-playground-sandbox",
    "METAFLOW_DEFAULT_DATASTORE": "s3",
    "METAFLOW_USER": "talebz",
    "METAFLOW_SERVICE_URL": "https://metaflow.int.sandbox-k8s.zg-aip.net/metaflow-service"
}
```

Then, within the `tests` directory, run `python -m pytest -s -n 3 run_integration_tests.py`. The parameter `-n` specifies the number of parallel tests. You'll likely need to change this if resource constraints are an issue.

# Github Tests

These tests are configured to automatically run whenever you push a commit to this Github repository. A mirrored Gitlab repository detects changes, pulls in the changes, and triggers a pipeline.

Currently, due to Gitlab's polling, it takes about 20 minutes for these tests to be triggered automatically. To trigger these tests manually, run:

`curl -X POST "https://gitlab.zgtools.net/api/v4/projects/20508/mirror/pull?private_token=[PRIVATE_TOKEN]"`

Please reach out to @hariharans on Slack (for Zillow employees) to obtain the private token to run on Zillow internal Gitlab infrastructure.

# Unit Tests

On top of the integration tests, we also have units tests. Currently, we only have unit tests for the
`@s3_sensor` decorator.
