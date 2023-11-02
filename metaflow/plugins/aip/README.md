### Commands used and References

#### Steps to run Metaflow on Kubeflow: 

#### Installing the correct forked version of Metaflow
To install Metaflow: 
```
pip3 install --user --upgrade git+https://github.com/zillow/metaflow.git@feature/aip
```

##### Configure Metaflow for laptop development
1. Configure a metaflow profile. To do this, create a file named as `config.json` under 
`~/.metaflowconfig/`) and set the required values. Required fields to run on Argo are shared below*.
2. Create your runs on Argo using the following command template: 
```
python <program-name.py> aip run
```

The AIP Metaflow plugin name remains for historical reasons, however it submits the 
Argo workflow directly to Argo.

##### Example METAFLOW_PROFILE:

Config file: `config.json` (to be saved under `~/.metaflowconfig`):
Contents of the config file:
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

To `run-on-aip` using this profile:
```
python hello.py aip run
```

To `generate-aip-yaml` using this profile:
```
python helloworld.py aip run --yaml-only --no-s3-code-package
```


#### What's happening inside the step_container_op:

We execute the above local orchestration commands after performing the necessary setup. The current setup includes the following:

- Copy the code package from S3 and untar it.
- Run the step command

### Foreach -> ParallelFor support
* [nested_parallelfor.ipynb](nested_parallelfor.ipynb)
* [metaflow_nested_foreach.ipynb](metaflow_nested_foreach.ipynb)
