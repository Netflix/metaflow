### Commands used and References

###### Last Updated: Aug 12th:

#### Steps to run Metaflow on Kubeflow: 

##### Option 1:
1. Configure a metaflow profile. To do this, create a file named as `config_<your-metaflow-profile-name>.json` under 
`~/.metaflowconfig/`) and set the required values. Required fields to run on KFP are shared below*.
2. Create your runs on KFP using the following command template: 
```
export METAFLOW_PROFILE=<your-metaflow-profile-name> && 
python <program-name.py> [run-on-kfp|generate-kfp-yaml] (--code-url <link-to-code>)
```

##### Option 2:
You can export the required variables* individually and run the python command:

*Required keys in `config_<profile-name>.json` to run on KFP are mentioned below:

```
{
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://<path-to-s3-bucket-root>",
    "METAFLOW_DEFAULT_DATASTORE": "s3",

    "METAFLOW_AWS_ARN" : "...", # required to be able to read s3 data when running within KFP
    "METAFLOW_AWS_S3_REGION" : "...", # specify s3 region being used

    "KFP_RUN_URL_PREFIX" : "https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details" # prefix of the URL preceeding the run-id to generate correct links to the generated runs on your cluster
}
```


##### Example using a METAFLOW_PROFILE named `sree` (option 1):

Config file: `config_sree.json` (to be saved under `~/.metaflowconfig`):
Contents of the config file:
```
{
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://workspace-zillow-analytics-stage/aip/metaflow",
    "METAFLOW_DEFAULT_DATASTORE": "s3",

    "METAFLOW_AWS_ARN" : "arn:aws:iam::170606514770:role/dev-zestimate-role",
    "METAFLOW_AWS_S3_REGION" : "us-west-2",

    "KFP_RUN_URL_PREFIX" : "https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details"
}
```

To `run-on-kfp` using this profile:
```
export METAFLOW_PROFILE=sree && 
python 00-helloworld/hello.py run-on-kfp 
    --experiment-name "MF-on-KFP-P2" 
    --run-name "hello_run" 
    --code-url https://raw.githubusercontent.com/zillow/metaflow/mf-on-kfp-2/metaflow/tutorials/00-helloworld/hello.py
    --namespace [your_namespace]
    --userid [your_userid]
```

To `generate-kfp-yaml` using this profile:
```
export METAFLOW_PROFILE=sree && 
python 00-helloworld/hello.py generate-kfp-yaml
      --code-url https://raw.githubusercontent.com/zillow/metaflow/mf-on-kfp-2/metaflow/tutorials/00-helloworld/hello.py  
```


#####  Example of `run-on-kfp` without configuring a profile (option 2):
```
export METAFLOW_AWS_ARN="arn:aws:iam::170606514770:role/dev-zestimate-role" && 
export METAFLOW_AWS_S3_REGION="us-west-2" && 
export METAFLOW_DATASTORE_SYSROOT_S3="s3://workspace-zillow-analytics-stage/aip/metaflow" && 
export KFP_RUN_URL_PREFIX="https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details/" && 
python 00-helloworld/hello.py run-on-kfp 
    --code-url="https://raw.githubusercontent.com/zillow/metaflow/state-integ-s3/metaflow/tutorials/00-helloworld/hello.py"
```

#### What's happening inside the step_container_op:

We execute the above local orchestration commands after performing the necessary setup. The current setup includes the following:

- Download the script to be run (needed as we aren't solving code packaging yet)
- Install the modified metaflow version (from Zillow's fork of Metaflow where we are pushing our changes)
- Set a KFP user
- Run the step command