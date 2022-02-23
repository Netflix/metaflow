# Airflow Integration 

```sh
# create python file
python section_exp.py airflow create a.py
```

## How does it work : 
1. Metaflow converts the flow into a intermediate JSON like format and then writes it to a python file template provided in `af_deploy.py`. 
2. The JSON in the compiled airflow file will be used to compile the Airflow DAG at runtime. Even though airflow provides `airflow.serialization.serialized_objects.SerializedDAG`, we cannot use this to directly serialize the DAG. This class can only be used in the scope of scheduler and webserver

## Some Pointers Before Running Scheduler

Before running `airflow scheduler` ensure that metaflow is installed in the PYTHONPATH. 

The DAG compiled from the flow will use API's exposed via metaflow to compile and interpret the DAG.  