# Airflow Integration 

## Compute Support : 
- K8s

## How does it work ? 
The cli provides a `airflow create` command which helps convert a metaflow flow into an Airflow Workflow file. In the below example `a.py` will be an airflow workflow. 
```sh
# create python file
python section_exp.py airflow create a.py
```
Once `a.py` is created it can be placed into the Airflow's dags folder. From there the tasks will be orchestrated onto kubernetes. 

## Details of workings. 

The [airflow_utils.py](airflow_utils.py) contains the utility functions/classes to create the workflow and export the workflow into dictionary format. 

The [airflow_compiler.py](airflow_compiler.py) leverages `airflow_utils.py` to the compile the workflow into dictionary format and then using mustache we create the airflow file. The [af_deploy.py](af_deploy.py) file has the python template. The [airflow_utils.py](airflow_utils.py) is pasted as is into the template. The code in the compiled workflow reusese the [airflow_utils.py](airflow_utils.py) code convert the dictionary format into airflow `Operator`s and `DAG`.

