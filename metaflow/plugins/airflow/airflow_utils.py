from collections import defaultdict
import os
import typing
import json
import time
import random
import hashlib
import re
from datetime import timedelta, datetime
import hashlib

LABEL_VALUE_REGEX = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9\-\_\.]{0,61}[a-zA-Z0-9])?$")


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
    return hashlib.md5(my_value.encode("utf-8")).hexdigest()


class AirflowDAGArgs(object):
    _arg_types = {
        "dag_id": "asdf",
        "description": "asdfasf",
        "schedule_interval": "*/2 * * * *",
        "start_date": datetime.now(),
        "catchup": False,
        "tags": [],
        "dagrun_timeout": timedelta(minutes=60 * 4),
        "default_args": {
            "owner": "some_username",
            "depends_on_past": False,
            "email": ["some_email"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "queue": "bash_queue",
            "pool": "backfill",
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
        "user_defined_filters": dict(hash=lambda my_value: hasher(my_value)),
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

    task_id = "arf-{{ ti.job_id }}"
    run_id = "arf-{{ run_id | hash }}"  # hash is added via the `user_defined_filters`
    attempt = "{{ task_instance.try_number - 1 }}"
    # Set dynamic env variables like run-id, task-id etc from here.
    env_vars = [
        client.V1EnvVar(name=v["name"], value=str(v["value"]))
        for v in operator_args.get("env_vars", [])
    ] + [
        client.V1EnvVar(name=k, value=str(v))
        for k, v in dict(
            METAFLOW_RUN_ID=run_id,
            METAFLOW_AIRFLOW_TASK_ID=task_id,
            METAFLOW_ATTEMPT_NUMBER=attempt,
        ).items()
    ]

    labels = {
        "metaflow/attempt": attempt,
        "metaflow/run_id": run_id,
        "metaflow/task_id": task_id,
    }
    volume_mounts = [
        client.V1VolumeMount(**v) for v in operator_args.get("volume_mounts", [])
    ]
    volumes = [client.V1Volume(**v) for v in operator_args.get("volumes", [])]
    args = {
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
        "secrets": operator_args.get("secrets", []),
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
        "affinity": None,  # kubernetes.client.models.v1_affinity.V1Affinity
        "config_file": None,
        "node_selectors": {},  # todo : Find difference between "node_selectors" / "node_selector"
        "node_selector": {},  # todo : Find difference between "node_selectors" / "node_selector"
        # image_pull_secrets : typing.Union[typing.List[kubernetes.client.models.v1_local_object_reference.V1LocalObjectReference], NoneType],
        "image_pull_secrets": operator_args.get(
            "image_pull_secrets", []
        ),  # todo : document what this is for ?
        "service_account_name": operator_args.get(
            "service_account_name", None
        ),  # todo : document what this is for ? ,  # todo : document what this is for ?
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
    return args


def get_k8s_operator():
    from airflow.contrib.operators.kubernetes_pod_operator import (
        KubernetesPodOperator,
    )

    return KubernetesPodOperator


class AirflowTask(object):
    def __init__(self, name, operator_type="kubernetes", flow_name=None):
        self.name = name
        self._operator_args = None
        self._operator_type = operator_type
        self._next = None
        self._flow_name = flow_name

    def set_operator_args(self, **kwargs):
        self._operator_args = kwargs
        return self

    @property
    def next_state(self):
        return self._next

    def next(self, out_func_name):
        self._next = out_func_name
        return self

    def to_dict(self):
        return {
            "name": self.name,
            "next": self._next,
            "operator_type": self._operator_type,
            "operator_args": self._operator_args,
        }

    @classmethod
    def from_dict(cls, jsd, flow_name=None):
        op_args = {} if not "operator_args" in jsd else jsd["operator_args"]
        return (
            cls(
                jsd["name"],
                operator_type=jsd["operator_type"]
                if "operator_type" in jsd
                else "kubernetes",
                flow_name=flow_name,
            )
            .next(jsd["next"])
            .set_operator_args(**op_args)
        )

    def _kubenetes_task(self):
        KubernetesPodOperator = get_k8s_operator()
        k8s_args = set_k8s_operator_args(
            self._flow_name, self.name, self._operator_args
        )
        return KubernetesPodOperator(**k8s_args)

    def to_task(self):
        # todo fix
        if self._operator_type == "kubernetes":
            return self._kubenetes_task()


class Workflow(object):
    def __init__(self, file_path=None, **kwargs):
        self._dag_instantiation_params = AirflowDAGArgs(**kwargs)
        self._file_path = file_path
        tree = lambda: defaultdict(tree)
        self.states = tree()

    def add_state(self, state):
        self.states[state.name] = state

    def to_dict(self):
        return dict(
            states={s: v.to_dict() for s, v in self.states.items()},
            dag_instantiation_params=self._dag_instantiation_params.to_dict(),
            file_path=self._file_path,
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data_dict):
        re_cls = cls(
            file_path=data_dict["file_path"],
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
        return re_cls

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        return cls.from_dict(data)

    def compile(self):
        from airflow import DAG

        dag = DAG(**self._dag_instantiation_params.arguements)
        curr_state = self.states["start"]
        curr_task = self.states["start"].to_task()
        prev_task = None
        # Todo : Assert that fileloc is required because the DAG export has that information.
        dag.fileloc = self._file_path if self._file_path is not None else dag.fileloc
        with dag:
            while curr_state is not None:
                curr_task = curr_state.to_task()
                if prev_task is not None:
                    prev_task >> curr_task

                if curr_state.next_state is None:
                    curr_state = None
                else:
                    curr_state = self.states[curr_state.next_state]
                prev_task = curr_task
        return dag
