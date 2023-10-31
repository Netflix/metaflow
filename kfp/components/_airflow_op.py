# Copyright 2019 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = [
    'create_component_from_airflow_op',
]

from typing import List

from ._python_op import _func_to_component_spec, _create_task_factory_from_component_spec

_default_airflow_base_image = 'apache/airflow:master-python3.6-ci'  #TODO: Update a production release image once they become available: https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-10+Multi-layered+and+multi-stage+official+Airflow+CI+image#AIP-10Multi-layeredandmulti-stageofficialAirflowCIimage-ProposedsetupoftheDockerHubandTravisCI . See https://issues.apache.org/jira/browse/AIRFLOW-5093


def create_component_from_airflow_op(
        op_class: type,
        base_image: str = _default_airflow_base_image,
        variable_output_names: List[str] = None,
        xcom_output_names: List[str] = None,
        modules_to_capture: List[str] = None):
    """Creates component function from an Airflow operator class. The inputs of
    the component are the same as the operator constructor parameters. By
    default the component has the following outputs: "Result", "Variables" and
    "XComs". "Variables" and "XComs" are serialized JSON maps of all variables
    and xcoms produced by the operator during the execution. Use the
    variable_output_names and xcom_output_names parameters to output individual
    variables/xcoms as separate outputs.

    Args:
        op_class: Reference to the Airflow operator class (e.g. EmailOperator or BashOperator) to convert to componenent.
        base_image: Optional. The container image to use for the component. Default is apache/airflow. The container image must have the same python version as the environment used to run create_component_from_airflow_op. The image should have python 3.5+ with airflow package installed.
        variable_output_names: Optional. A list of Airflow "variables" produced by the operator that should be returned as separate outputs.
        xcom_output_names: Optional. A list of Airflow "XComs" produced by the operator that should be returned as separate outputs.
        modules_to_capture: Optional. A list of names of additional modules that the operator depends on. By default only the module containing the operator class is captured. If the operator class uses the code from another module, the name of that module can be specified in this list.
    """
    component_spec = _create_component_spec_from_airflow_op(
        op_class=op_class,
        base_image=base_image,
        variables_to_output=variable_output_names,
        xcoms_to_output=xcom_output_names,
        modules_to_capture=modules_to_capture,
    )
    task_factory = _create_task_factory_from_component_spec(component_spec)
    return task_factory


def _create_component_spec_from_airflow_op(
    op_class: type,
    base_image: str = _default_airflow_base_image,
    result_output_name: str = 'Result',
    variables_dict_output_name: str = 'Variables',
    xcoms_dict_output_name: str = 'XComs',
    variables_to_output: List[str] = None,
    xcoms_to_output: List[str] = None,
    modules_to_capture: List[str] = None,
):
    variables_output_names = variables_to_output or []
    xcoms_output_names = xcoms_to_output or []
    modules_to_capture = modules_to_capture or [op_class.__module__]
    modules_to_capture.append(_run_airflow_op.__module__)

    output_names = []
    if result_output_name is not None:
        output_names.append(result_output_name)
    if variables_dict_output_name is not None:
        output_names.append(variables_dict_output_name)
    if xcoms_dict_output_name is not None:
        output_names.append(xcoms_dict_output_name)
    output_names.extend(variables_output_names)
    output_names.extend(xcoms_output_names)

    from collections import namedtuple
    returnType = namedtuple('AirflowOpOutputs', output_names)

    def _run_airflow_op_closure(*op_args, **op_kwargs) -> returnType:
        (result, variables, xcoms) = _run_airflow_op(op_class, *op_args,
                                                     **op_kwargs)

        output_values = {}

        import json
        if result_output_name is not None:
            output_values[result_output_name] = str(result)
        if variables_dict_output_name is not None:
            output_values[variables_dict_output_name] = json.dumps(variables)
        if xcoms_dict_output_name is not None:
            output_values[xcoms_dict_output_name] = json.dumps(xcoms)
        for name in variables_output_names:
            output_values[name] = variables[name]
        for name in xcoms_output_names:
            output_values[name] = xcoms[name]

        return returnType(**output_values)

    # Hacking the function signature so that correct component interface is generated
    import inspect
    parameters = inspect.signature(op_class).parameters.values()
    #Filtering out `*args` and `**kwargs` parameters that some operators have
    parameters = [
        param for param in parameters
        if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]
    sig = inspect.Signature(
        parameters=parameters,
        return_annotation=returnType,
    )
    _run_airflow_op_closure.__signature__ = sig
    _run_airflow_op_closure.__name__ = op_class.__name__

    return _func_to_component_spec(
        _run_airflow_op_closure,
        base_image=base_image,
        use_code_pickling=True,
        modules_to_capture=modules_to_capture)


def _run_airflow_op(Op, *op_args, **op_kwargs):
    from airflow.utils import db
    db.initdb()

    from datetime import datetime
    from airflow import DAG, settings
    from airflow.models import TaskInstance, Variable, XCom

    dag = DAG(dag_id='anydag', start_date=datetime.now())
    task = Op(*op_args, **op_kwargs, dag=dag, task_id='anytask')
    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())

    variables = {
        var.id: var.val for var in settings.Session().query(Variable).all()
    }
    xcoms = {msg.key: msg.value for msg in settings.Session().query(XCom).all()}
    return (result, variables, xcoms)
