# How does xcom push work in K8s Operator : https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#how-does-xcom-work
import os
import json

K8S_XCOM_DIR_PATH = "/airflow/xcom"


def safe_mkdir(dir):
    try:
        os.makedirs(dir)
    except FileExistsError:
        pass


def push_xcom_values(xcom_dict):
    safe_mkdir(K8S_XCOM_DIR_PATH)
    with open(os.path.join(K8S_XCOM_DIR_PATH, "return.json"), "w") as f:
        json.dump(xcom_dict, f)
