from collections import defaultdict
import json
from datetime import timedelta, datetime


class AirflowDAGArgs(object):
    _arg_types = {
        "dag_id": "asdf",
        "description": "asdfasf",
        "schedule_interval": "*/2 * * * *",
        "start_date": datetime.now(),
        "catchup": False,
        "tags": [],
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
            "end_date": datetime(2016, 1, 1),
            "wait_for_downstream": False,
            "sla": timedelta(hours=2),
            "execution_timeout": timedelta(seconds=300),
            "trigger_rule": "all_success"
            # 'dag': dag,
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
        },
    }

    def __init__(self, **kwargs):
        self._args = kwargs

    @property
    def arguements(self):
        return self._args

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


class AirflowTask(object):
    def __init__(self, name):
        self.name = name
        self._next = None
        self._operator = None
        self._operator_args = None

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
        }

    @classmethod
    def from_dict(cls, jsd):
        return cls(jsd["name"]).next(jsd["next"])

    def to_task(self):
        # todo fix
        from airflow.operators.bash import BashOperator

        return BashOperator(
            task_id=self.name,
            depends_on_past=True,
            bash_command="sleep 1",
        )


class Workflow(object):
    def __init__(self, file_path=None, **kwargs):
        self._dag_instantiation_params = AirflowDAGArgs(**kwargs)
        self._file_path = file_path
        tree = lambda: defaultdict(tree)
        self.states = tree()

    def add_state(self, state):
        self.states[state.name] = state

    def to_json(self):
        return json.dumps(
            dict(
                states={s: v.to_dict() for s, v in self.states.items()},
                dag_instantiation_params=self._dag_instantiation_params.to_dict(),
                file_path=self._file_path,
            )
        )

    @classmethod
    def from_json(cls, json_string):
        data = json.loads(json_string)
        re_cls = cls(
            file_path=data["file_path"],
        )
        for sd in data["states"].values():
            re_cls.add_state(AirflowTask.from_dict(sd))
        re_cls._dag_instantiation_params = AirflowDAGArgs.from_dict(
            data["dag_instantiation_params"]
        )
        return re_cls

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
