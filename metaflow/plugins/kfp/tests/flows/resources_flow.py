import os
import pprint
import subprocess
import time
from typing import Dict, List
from multiprocessing.shared_memory import SharedMemory

from kubernetes.client import (
    V1EnvVar,
    V1EnvVarSource,
    V1ObjectFieldSelector,
    V1ResourceFieldSelector,
)

from metaflow import FlowSpec, Parameter, current, environment, resources, step
from metaflow._vendor import click
from metaflow.datatools.s3 import S3


def get_env_vars(env_resources: Dict[str, str]) -> List[V1EnvVar]:
    res = []
    for name, resource in env_resources.items():
        res.append(
            V1EnvVar(
                # this is used by some functions of operator-sdk
                # it uses this environment variable to get the pods
                name=name,
                value_from=V1EnvVarSource(
                    resource_field_ref=V1ResourceFieldSelector(
                        container_name="main",
                        resource=resource,
                        divisor="1m" if "cpu" in resource else "1",
                    )
                ),
            )
        )
    return res


kubernetes_vars = get_env_vars(
    {
        "LOCAL_STORAGE": "requests.ephemeral-storage",
        "LOCAL_STORAGE_LIMIT": "limits.ephemeral-storage",
        "CPU": "requests.cpu",
        "CPU_LIMIT": "limits.cpu",
        "MEMORY": "requests.memory",
        "MEMORY_LIMIT": "limits.memory",
    }
)
kubernetes_vars.append(
    V1EnvVar(
        name="MY_POD_NAME",
        value_from=V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="metadata.name")
        ),
    )
)

annotations = {
    "metaflow.org/flow_name": "MF_NAME",
    "metaflow.org/step": "MF_STEP",
    "metaflow.org/run_id": "MF_RUN_ID",
    "metaflow.org/experiment": "MF_EXPERIMENT",
    "metaflow.org/tag_metaflow_test": "MF_TAG_METAFLOW_TEST",
    "metaflow.org/tag_test_t1": "MF_TAG_TEST_T1",
    "metaflow.org/tag_test_sys_t1": "MF_SYS_TAG_TEST_T1",
}
for annotation, env_name in annotations.items():
    kubernetes_vars.append(
        V1EnvVar(
            name=env_name,
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(
                    field_path=f"metadata.annotations['{annotation}']"
                )
            ),
        )
    )

labels = {
    "aip.zillowgroup.net/kfp-pod-default": "KF_POD_DEFAULT",
    "tags.ledger.zgtools.net/ai-flow-name": "AI_FLOW_NAME",
    "tags.ledger.zgtools.net/ai-step-name": "AI_STEP_NAME",
    "tags.ledger.zgtools.net/ai-experiment-name": "AI_EXPERIMENT_NAME",
    "zodiac.zillowgroup.net/owner": "ZODIAC_OWNER",
}
for label, env_name in labels.items():
    kubernetes_vars.append(
        V1EnvVar(
            name=env_name,
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(
                    field_path=f"metadata.labels['{label}']"
                )
            ),
        )
    )

sub_dict = dict(x=1, y="hello")
default_dict = dict(a=1, hello="world", sub=sub_dict)

# introduce our own type to test that it works, because JSONType is special handled
# and JSON serialized in kfp_cli.py
class TestTypeClass(click.ParamType):
    name = "TestType"

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            import json

            return json.loads(value)
        else:
            return value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "Dataset"


class ResourcesFlow(FlowSpec):
    json_param: Dict = Parameter(
        "json_param", default=default_dict, type=TestTypeClass()
    )

    @resources(
        local_storage="242",
        cpu="0.6",
        memory="1G",
    )
    @environment(  # pylint: disable=E1102
        vars={"MY_ENV": "value"}, kubernetes_vars=kubernetes_vars
    )
    @step
    def start(self):
        assert self.json_param == default_dict
        pprint.pprint(dict(os.environ))
        print("=====")

        # test simple environment var
        assert os.environ.get("MY_ENV") == "value"

        # test kubernetes_vars
        assert "resourcesflow" in os.environ.get("MY_POD_NAME")
        assert os.environ.get("CPU") == "600"
        assert os.environ.get("CPU_LIMIT") == "600"
        assert os.environ.get("LOCAL_STORAGE") == "242000000"
        assert os.environ.get("LOCAL_STORAGE_LIMIT") == "242000000"
        assert os.environ.get("MEMORY") == "1000000000"
        assert os.environ.get("MEMORY_LIMIT") == "1000000000"

        assert os.environ.get("MF_NAME") == current.flow_name
        assert os.environ.get("MF_STEP") == current.step_name
        assert os.environ.get("MF_RUN_ID") == current.run_id
        assert os.environ.get("MF_EXPERIMENT") == "metaflow_test"
        assert os.environ.get("MF_TAG_METAFLOW_TEST") == "true"
        assert os.environ.get("MF_TAG_TEST_T1") == "true"
        assert os.environ.get("MF_SYS_TAG_TEST_T1") == "sys_tag_value"

        assert os.environ.get("KF_POD_DEFAULT") == "true"

        assert os.environ.get("AI_FLOW_NAME") == current.flow_name
        assert os.environ.get("AI_STEP_NAME") == current.step_name
        assert os.environ.get("AI_EXPERIMENT_NAME") == "metaflow_test"

        assert os.environ.get("ZODIAC_OWNER")

        self.items = [1, 2]
        self.next(self.foreach_step, foreach="items")

    @environment(vars={"MY_ENV": "value"})  # pylint: disable=E1102
    @resources(volume="11G", shared_memory="200M")
    @step
    def foreach_step(self):
        # test simple environment var
        assert os.environ.get("MY_ENV") == "value"

        with S3(run=self) as s3:
            value = "123"
            s3.put("foo", value)
            assert s3.get("foo").text == value
            print(f"{s3._tmpdir=}")
            assert "/opt/metaflow_volume" in s3._tmpdir

        output = subprocess.check_output(
            "df -h | grep /opt/metaflow_volume", shell=True
        )
        assert "11G" in str(output)

        # Testing shared memory that
        # - exceeds docker default shared memory size of 64 MB - 150 MB in this case
        # - test on foreach step to make sure volume name collision is not an issue
        shared_memory_block = SharedMemory(create=True, size=157286400)
        shared_memory_block.close()
        shared_memory_block.unlink()  # destroy shared memory block

        self.next(self.join_step)

    @resources(volume="12G")
    @step
    def join_step(self, inputs):
        output = subprocess.check_output(
            "df -h | grep /opt/metaflow_volume", shell=True
        )
        assert "12G" in str(output)
        self.next(self.split_step)

    @step
    def split_step(self):
        self.items = [1, 2]
        self.next(self.shared_volume_foreach_step, foreach="items")

    @resources(volume="13G", volume_mode="ReadWriteMany")
    @step
    def shared_volume_foreach_step(self):
        output = subprocess.check_output(
            "df -h | grep /opt/metaflow_volume", shell=True
        )
        assert "13G" in str(output)

        file_path = "/opt/metaflow_volume/test.txt"
        message = "hello world!"

        # validate the volume is shared across the foreach splits
        if self.input == 1:
            with open(file_path, "w") as f:
                f.write(message)
        else:
            while not os.path.exists(file_path):
                time.sleep(1)
                print(".")

            with open(file_path, "r") as f:
                read_lines = f.readlines()
                print("read_lines", read_lines)
                assert message == read_lines[0]

        self.next(self.shared_volume_join_step)

    @step
    def shared_volume_join_step(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        print("All done.")


if __name__ == "__main__":
    ResourcesFlow()
