from collections import defaultdict
import os
import typing
import json
import time
import random
import hashlib
import re
from datetime import timedelta, datetime


class KubernetesProviderNotFound(Exception):
    headline = "Kubernetes provider not found"


LABEL_VALUE_REGEX = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9\-\_\.]{0,61}[a-zA-Z0-9])?$")

TASK_ID_XCOM_KEY = "metaflow_task_id"
RUN_ID_LEN = 12
TASK_ID_LEN = 8

# AIRFLOW_TASK_ID_TEMPLATE_VALUE will work for linear/branched workflows.
# ti.task_id is the stepname in metaflow code.
# AIRFLOW_TASK_ID_TEMPLATE_VALUE uses a jinja filter called `task_id_creator` which helps
# concatenate the string using a `/`. Since run-id will keep changing and stepname will be
# the same task id will change. Since airflow doesn't encourage dynamic rewriting of dags
# we can rename steps in a foreach with indexes (eg. `stepname-$index`) to create those steps.
# Hence : Foreachs will require some special form of plumbing.
# https://stackoverflow.com/questions/62962386/can-an-airflow-task-dynamically-generate-a-dag-at-runtime
AIRFLOW_TASK_ID_TEMPLATE_VALUE = "arf-{{ [run_id, ti.task_id ] | task_id_creator  }}"


def sanitize_label_value(val):
    # Label sanitization: if the value can be used as is, return it as is.
    # If it can't, sanitize and add a suffix based on hash of the original
    # value, replace invalid chars and truncate.
    #
    # The idea here is that even if there are non-allowed chars in the same
    # position, this function will likely return distinct values, so you can
    # still filter on those. For example, "alice$" and "alice&" will be
    # sanitized into different values "alice_b3f201" and "alice_2a6f13".
    if val == "" or LABEL_VALUE_REGEX.match(val):
        return val
    hash = hashlib.sha256(val.encode("utf-8")).hexdigest()

    # Replace invalid chars with dots, and if the first char is
    # non-alphahanumeric, replace it with 'u' to make it valid
    sanitized_val = re.sub("^[^A-Z0-9a-z]", "u", re.sub(r"[^A-Za-z0-9.\-_]", "_", val))
    return sanitized_val[:57] + "-" + hash[:5]


def hasher(my_value):
    return hashlib.md5(my_value.encode("utf-8")).hexdigest()[:RUN_ID_LEN]


def task_id_creator(lst):
    # This is a filter which creates a hash of the run_id/step_name string.
    # Since run_ids in airflow are constants, they don't create an issue with the
    #
    return hashlib.md5("/".join(lst).encode("utf-8")).hexdigest()[:TASK_ID_LEN]


def json_dump(val):
    return json.dumps(val)


class AirflowDAGArgs(object):
    # _arg_types This object helps map types of
    # different keys that need to be parsed. None of the "values" in this
    # dictionary are being used. But the "types" of the values of are used when
    # reparsing the arguments from the config variable.
    _arg_types = {
        "dag_id": "asdf",
        "description": "asdfasf",
        "schedule_interval": "*/2 * * * *",
        "start_date": datetime.now(),
        "catchup": False,
        "tags": [],
        "max_retry_delay": "",
        "dagrun_timeout": timedelta(minutes=60 * 4),
        "default_args": {
            "owner": "some_username",
            "depends_on_past": False,
            "email": ["some_email"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            # Todo : find defaults
            "retry_delay": timedelta(seconds=10),
            "queue": "bash_queue",  #  which queue to target when running this job. Not all executors implement queue management, the CeleryExecutor does support targeting specific queues.
            "pool": "backfill",  # the slot pool this task should run in, slot pools are a way to limit concurrency for certain tasks
            "priority_weight": 10,
            "wait_for_downstream": False,
            "sla": timedelta(hours=2),
            "execution_timeout": timedelta(minutes=10),
            "trigger_rule": "all_success"
            # 'dag': dag,
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
        },
    }

    metaflow_centric_args = {
        # Reference for user_defined_filters : https://stackoverflow.com/a/70175317
        "user_defined_filters": dict(
            hash=lambda my_value: hasher(my_value),
            task_id_creator=lambda v: task_id_creator(v),
            json_dump=lambda val: json_dump(val),
        ),
    }

    def __init__(self, **kwargs):
        self._args = kwargs

    @property
    def arguements(self):
        return dict(**self._args, **self.metaflow_centric_args)

    def _serialize_args(self):
        def parse_args(dd):
            data_dict = {}
            for k, v in dd.items():
                if k == "default_args":
                    data_dict[k] = parse_args(v)
                elif isinstance(v, datetime):
                    data_dict[k] = v.isoformat()
                elif isinstance(v, timedelta):
                    data_dict[k] = dict(seconds=v.total_seconds())
                else:
                    data_dict[k] = v
            return data_dict

        return parse_args(self._args)

    @classmethod
    def from_dict(cls, data_dict):
        def parse_args(dd, type_check_dict):
            kwrgs = {}
            for k, v in dd.items():
                if k not in type_check_dict:
                    kwrgs[k] = v
                    continue
                if k == "default_args":
                    kwrgs[k] = parse_args(v, type_check_dict[k])
                elif isinstance(type_check_dict[k], datetime):
                    kwrgs[k] = datetime.fromisoformat(v)
                elif isinstance(type_check_dict[k], timedelta):
                    kwrgs[k] = timedelta(**v)
                else:
                    kwrgs[k] = v
            return kwrgs

        return cls(**parse_args(data_dict, cls._arg_types))

    def to_dict(self):
        dd = self._serialize_args()
        return dd


def generate_rfc1123_name(flow_name, step_name):
    """
    Generate RFC 1123 compatible name. Specifically, the format is:
        <let-or-digit>[*[<let-or-digit-or-hyphen>]<let-or-digit>]

    The generated name consists from a human-readable prefix, derived from
    flow/step/task/attempt, and a hash suffux.
    """
    unique_str = "%s-%d" % (str(time.time()), random.randint(0, 1000))

    long_name = "-".join([flow_name, step_name, unique_str])
    hash = hashlib.sha256(long_name.encode("utf-8")).hexdigest()

    if long_name.startswith("_"):
        # RFC 1123 names can't start with hyphen so slap an extra prefix on it
        sanitized_long_name = "u" + long_name.replace("_", "-").lower()
    else:
        sanitized_long_name = long_name.replace("_", "-").lower()

    # the name has to be under 63 chars total
    return sanitized_long_name[:57] + "-" + hash[:5]


def set_k8s_operator_args(flow_name, step_name, operator_args):
    from kubernetes import client
    from airflow.kubernetes.secret import Secret

    task_id = AIRFLOW_TASK_ID_TEMPLATE_VALUE
    run_id = "arf-{{ run_id | hash }}"  # hash is added via the `user_defined_filters`
    attempt = "{{ task_instance.try_number - 1 }}"
    # Set dynamic env variables like run-id, task-id etc from here.
    env_vars = (
        [
            client.V1EnvVar(name=v["name"], value=str(v["value"]))
            for v in operator_args.get("env_vars", [])
        ]
        + [
            client.V1EnvVar(name=k, value=str(v))
            for k, v in dict(
                METAFLOW_RUN_ID=run_id,
                METAFLOW_AIRFLOW_TASK_ID=task_id,
                METAFLOW_AIRFLOW_DAG_RUN_ID="{{run_id}}",
                METAFLOW_AIRFLOW_JOB_ID="{{ti.job_id}}",
                METAFLOW_ATTEMPT_NUMBER=attempt,
            ).items()
        ]
        + [
            client.V1EnvVar(
                name=k,
                value_from=client.V1EnvVarSource(
                    field_ref=client.V1ObjectFieldSelector(field_path=str(v))
                ),
            )
            for k, v in {
                "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
            }.items()
        ]
    )

    labels = {
        "metaflow/attempt": attempt,
        "metaflow/run_id": run_id,
        "metaflow/task_id": task_id,
    }
    volume_mounts = [
        client.V1VolumeMount(**v) for v in operator_args.get("volume_mounts", [])
    ]
    volumes = [client.V1Volume(**v) for v in operator_args.get("volumes", [])]
    secrets = [
        Secret("env", secret, secret) for secret in operator_args.get("secrets", [])
    ]
    args = {
        # "on_retry_callback": retry_callback,
        "namespace": operator_args.get("namespace", "airflow"),
        "image": operator_args.get("image", "python"),
        "name": generate_rfc1123_name(flow_name, step_name),
        "task_id": step_name,
        "random_name_suffix": None,
        "cmds": operator_args.get("cmds", []),
        "arguments": operator_args.get("arguments", []),
        "ports": operator_args.get("ports", []),
        "volume_mounts": volume_mounts,
        "volumes": volumes,
        "env_vars": env_vars,
        "env_from": operator_args.get("env_from", []),
        "secrets": secrets,
        "in_cluster": operator_args.get(
            "in_cluster", False
        ),  # Todo : Document what is this ?
        "cluster_context": None,  # Todo : Document what is this ?
        "labels": operator_args.get("labels", {}),  # Todo : Document what is this ?
        "reattach_on_restart": False,
        "startup_timeout_seconds": 120,
        "get_logs": True,  # This needs to be set to True to ensure that doesn't error out looking for xcom
        "image_pull_policy": None,  # todo : document what is this ?
        "annotations": {},  # todo : document what is this ?
        "resources": client.V1ResourceRequirements(
            requests={
                "cpu": operator_args.get("cpu", 1),
                "memory": operator_args.get("memory", "2000M"),
            }
        ),  # kubernetes.client.models.v1_resource_requirements.V1ResourceRequirements
        "retries": operator_args.get("retries", 0),  # Base operator command
        "retry_exponential_backoff": False,  # todo : should this be a arg we allow on CLI.
        "affinity": None,  # kubernetes.client.models.v1_affinity.V1Affinity
        "config_file": None,
        "node_selectors": {},  # todo : Find difference between "node_selectors" / "node_selector"
        "node_selector": {},  # todo : Find difference between "node_selectors" / "node_selector"
        # image_pull_secrets : typing.Union[typing.List[kubernetes.client.models.v1_local_object_reference.V1LocalObjectReference], NoneType],
        "image_pull_secrets": operator_args.get(
            "image_pull_secrets", []
        ),  # todo : document what this is for ?
        "service_account_name": operator_args.get(  # Service account names can be essential for passing reference to IAM roles etc.
            "service_account_name", None
        ),
        "is_delete_operator_pod": operator_args.get(
            "is_delete_operator_pod", False
        ),  # todo : document what this is for ?
        "hostnetwork": False,  # todo : document what this is for ?
        "tolerations": None,  # typing.Union[typing.List[kubernetes.client.models.v1_toleration.V1Toleration], NoneType],
        "security_context": {},
        "dnspolicy": None,  # todo : document what this is for ?
        "schedulername": None,  # todo : document what this is for ?
        "full_pod_spec": None,  # typing.Union[kubernetes.client.models.v1_pod.V1Pod, NoneType]
        "init_containers": None,  # typing.Union[typing.List[kubernetes.client.models.v1_container.V1Container], NoneType],
        "log_events_on_failure": False,
        "do_xcom_push": True,
        "pod_template_file": None,  # todo : find out what this will do ?
        "priority_class_name": None,  # todo : find out what this will do ?
        "pod_runtime_info_envs": None,  # todo : find out what this will do ?
        "termination_grace_period": None,  # todo : find out what this will do ?
        "configmaps": None,  # todo : find out what this will do ?
    }
    args["labels"].update(labels)
    if operator_args.get("execution_timeout", None):
        args["execution_timeout"] = timedelta(
            **operator_args.get(
                "execution_timeout",
            )
        )
    if operator_args.get("retry_delay", None):
        args["retry_delay"] = timedelta(**operator_args.get("retry_delay"))
    return args


def get_k8s_operator():

    try:
        from airflow.contrib.operators.kubernetes_pod_operator import (
            KubernetesPodOperator,
        )
    except ImportError:
        try:
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
                KubernetesPodOperator,
            )
        except ImportError as e:
            # todo : Fix error messages.
            raise KubernetesProviderNotFound(
                "Running this DAG requires KubernetesPodOperator. "
                "Install Airflow Kubernetes provider using : "
                "`pip install apache-airflow-providers-cncf-kubernetes`"
            )

    return KubernetesPodOperator


class AirflowTask(object):
    def __init__(self, name, operator_type="kubernetes", flow_name=None):
        self.name = name
        self._operator_args = None
        self._operator_type = operator_type
        self._flow_name = flow_name

    def set_operator_args(self, **kwargs):
        self._operator_args = kwargs
        return self

    def to_dict(self):
        return {
            "name": self.name,
            "operator_type": self._operator_type,
            "operator_args": self._operator_args,
        }

    @classmethod
    def from_dict(cls, jsd, flow_name=None):
        op_args = {} if not "operator_args" in jsd else jsd["operator_args"]
        return cls(
            jsd["name"],
            operator_type=jsd["operator_type"]
            if "operator_type" in jsd
            else "kubernetes",
            flow_name=flow_name,
        ).set_operator_args(**op_args)

    def _kubenetes_task(self):
        KubernetesPodOperator = get_k8s_operator()
        k8s_args = set_k8s_operator_args(
            self._flow_name, self.name, self._operator_args
        )
        return KubernetesPodOperator(**k8s_args)

    def to_task(self):
        if self._operator_type == "kubernetes":
            return self._kubenetes_task()


class Workflow(object):
    def __init__(self, file_path=None, graph_structure=None, **kwargs):
        self._dag_instantiation_params = AirflowDAGArgs(**kwargs)
        self._file_path = file_path
        tree = lambda: defaultdict(tree)
        self.states = tree()
        self.metaflow_params = None
        self.graph_structure = graph_structure

    def set_parameters(self, params):
        self.metaflow_params = params

    def add_state(self, state):
        self.states[state.name] = state

    def to_dict(self):
        return dict(
            graph_structure=self.graph_structure,
            states={s: v.to_dict() for s, v in self.states.items()},
            dag_instantiation_params=self._dag_instantiation_params.to_dict(),
            file_path=self._file_path,
            metaflow_params=self.metaflow_params,
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data_dict):
        re_cls = cls(
            file_path=data_dict["file_path"],
            graph_structure=data_dict["graph_structure"],
        )
        re_cls._dag_instantiation_params = AirflowDAGArgs.from_dict(
            data_dict["dag_instantiation_params"]
        )

        for sd in data_dict["states"].values():
            re_cls.add_state(
                AirflowTask.from_dict(
                    sd, flow_name=re_cls._dag_instantiation_params.arguements["dag_id"]
                )
            )
        re_cls.set_parameters(data_dict["metaflow_params"])
        return re_cls

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        return cls.from_dict(data)

    def _construct_params(self):
        from airflow.models.param import Param

        if self.metaflow_params is None:
            return {}
        param_dict = {}
        for p in self.metaflow_params:
            name = p["name"]
            del p["name"]
            param_dict[name] = Param(**p)
        return param_dict

    def compile(self):
        from airflow import DAG

        params_dict = self._construct_params()
        # DAG Params can be seen here :
        # https://airflow.apache.org/docs/apache-airflow/2.0.0/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
        # Airflow 2.0.0 Allows setting Params.
        dag = DAG(params=params_dict, **self._dag_instantiation_params.arguements)
        dag.fileloc = self._file_path if self._file_path is not None else dag.fileloc

        def add_node(node, parents, dag):
            """
            A recursive function to traverse the specialized
            graph_structure datastructure.
            """
            if type(node) == str:
                task = self.states[node].to_task()
                if parents:
                    for parent in parents:
                        parent >> task
                return [task]  # Return Parent

            # this means a split from parent
            if type(node) == list:
                # this means branching since everything within the list is a list
                if all(isinstance(n, list) for n in node):
                    curr_parents = parents
                    parent_list = []
                    for node_list in node:
                        last_parent = add_node(node_list, curr_parents, dag)
                        parent_list.extend(last_parent)
                    return parent_list
                else:
                    # this means no branching and everything within the list is not a list and can be actual nodes.
                    curr_parents = parents
                    for node_x in node:
                        curr_parents = add_node(node_x, curr_parents, dag)
                    return curr_parents

        with dag:
            parent = None
            for node in self.graph_structure:
                parent = add_node(node, parent, dag)

        return dag
