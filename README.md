## Metaflow-Argo plugin
With the argo plugin it is possible to run metaflow scripts also on hyperscalers other than AWS by using Argo/K8S.

### Getting Started

* Install Metaflow from current repo which includes the argo plugin:
    ``` 
    git clone git@github.wdf.sap.corp:AI/metaflow-argo.git
    python3 setup.py install
    ```
  
* Configure S3\
see [Environment Variables](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Argo#enviroment-variables--config)
 
* Run metaflow pipeline:\
`python3 <YOUR-SCRIPT>.py argo create -only-yaml > <FILE>.yaml`\
\
see "mnist" example in tutorials folder:\
`python3 metaflow/tutorials/mnist/mnist-training.py argo create -only-yaml > mnist.yaml`

* Execute argo workflow\
submit yaml file to [argo dashboard](https://argo.intber.eu-central-1.mlf-aws-dev.com/workflows/argo-workflow-examples)


### Conda support
* Install [(mini)conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
    * you can activate the conda environment or simply add the conda executable path:\
    ``` export PATH="$PATH:$HOME/miniconda/bin" ```
* Run with conda environment parameter:\
  ```python3 <YOUR-SCRIPT>.py --environment=conda argo create -only-yaml > <FILE>.yaml```
  
*Note*: By default your local python version is applied. Make sure python version is not conflicting between the step's
docker image and the conda environment.\
E.g.\
 ``` @argo_base(image='python:3.7')```\
 ``` @conda_base(python='3.7.9') ```

### Links

You can find more documentation here:

* [Metaflow](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Metaflow)
* [Conda](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Conda)
* [Local installation of Argo](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Argo)
