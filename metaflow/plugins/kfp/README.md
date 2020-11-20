### Commands used and References

#### Steps to run Metaflow on Kubeflow: 

#### Installing the correct forked version of Metaflow AND KFP
Currently, to run Metaflow on our internal KFP, you need to install the correct versions of both.

To install KFP:
```
pip3 install 'git+https://github.com/alexlatchford/pipelines@alexla/AIP-1676#egg=kfp&subdirectory=sdk/python'
```

To install Metaflow: 
```
pip3 install --user --upgrade git+https://github.com/zillow/metaflow.git@feature/kfp
```

##### Option 1:
1. Configure a metaflow profile. To do this, create a file named as `config.json` under 
`~/.metaflowconfig/`) and set the required values. Required fields to run on KFP are shared below*.
2. Create your runs on KFP using the following command template: 
```
python <program-name.py> kfp run
```

##### Option 2:
You can export the required variables* individually and run the python command:

*Required keys in `config_<profile-name>.json` to run on KFP are mentioned below:

```
{
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://<path-to-s3-bucket-root>",
    "METAFLOW_DEFAULT_DATASTORE": "s3",
    "KFP_RUN_URL_PREFIX" : "https://kubeflow.corp.dev.zg-aip.net/",
    "KFP_SDK_NAMESPACE": "example_namespace",
    "KFP_SDK_USERID": "[YOUR_NAME]@email.com"
}
```

NOTE: If you provide either the KFP namespace or the user ID both in the config file/as environment vars (option 1) and in the CLI (option 2), 
the CLI argument will be selected. This will allow for faster iteration for the AS. In the examples here, we have provided these arguments both 
in the CLI and as environment vars/config file values. Regarding the `api_namespace`, you can either provide it either through environment variables 
or through the CLI, and the CLI value will take precedence. However, if you don't provide it at either place, it will be defaulted to "kubeflow".


##### Example using a METAFLOW_PROFILE:

Config file: `config.json` (to be saved under `~/.metaflowconfig`):
Contents of the config file:
```
{
    "KFP_RUN_URL_PREFIX": "https://kubeflow.corp.dev.zg-aip.net/",
    "KFP_SDK_NAMESPACE": "aip-example",
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://aip-example-dev/metaflow",
    "METAFLOW_DEFAULT_DATASTORE": "local",
    "METAFLOW_USER": "talebz@zillowgroup.com"
}
```

To `run-on-kfp` using this profile:
```
python hello.py kfp run
```

To `generate-kfp-yaml` using this profile:
```
python helloworld.py kfp run --yaml-only --no-s3-code-package
```


#### What's happening inside the step_container_op:

We execute the above local orchestration commands after performing the necessary setup. The current setup includes the following:

- Copy the code package from S3 and untar it.
- Set a KFP user
- Run the step command

### Foreach -> ParallelFor support
* [nested_parallelfor.ipynb](nested_parallelfor.ipynb)
* [metaflow_nested_foreach.ipynb](metaflow_nested_foreach.ipynb)
