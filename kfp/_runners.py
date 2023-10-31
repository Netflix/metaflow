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
    "run_pipeline_func_on_cluster",
    "run_pipeline_func_locally",
]

from typing import Callable, List, Mapping, Optional

from . import Client, LocalClient, dsl


def run_pipeline_func_on_cluster(
    pipeline_func: Callable,
    arguments: Mapping[str, str],
    run_name: str = None,
    experiment_name: str = None,
    kfp_client: Client = None,
    pipeline_conf: dsl.PipelineConf = None,
):
    """Runs pipeline on KFP-enabled Kubernetes cluster.

    This command compiles the pipeline function, creates or gets an experiment
    and submits the pipeline for execution.

    Feature stage:
    [Alpha](https://github.com/kubeflow/pipelines/blob/07328e5094ac2981d3059314cc848fbb71437a76/docs/release/feature-stages.md#alpha)

    Args:
      pipeline_func: A function that describes a pipeline by calling components
      and composing them into execution graph.
      arguments: Arguments to the pipeline function provided as a dict.
      run_name: Optional. Name of the run to be shown in the UI.
      experiment_name: Optional. Name of the experiment to add the run to.
      kfp_client: Optional. An instance of kfp.Client configured for the desired
        KFP cluster.
      pipeline_conf: Optional. kfp.dsl.PipelineConf instance. Can specify op
        transforms, image pull secrets and other pipeline-level configuration
        options.
    """
    kfp_client = kfp_client or Client()
    return kfp_client.create_run_from_pipeline_func(pipeline_func, arguments,
                                                    run_name, experiment_name,
                                                    pipeline_conf)


def run_pipeline_func_locally(
        pipeline_func: Callable,
        arguments: Mapping[str, str],
        local_client: Optional[LocalClient] = None,
        pipeline_root: Optional[str] = None,
        execution_mode: LocalClient.ExecutionMode = LocalClient.ExecutionMode(),
):
    """Runs a pipeline locally, either using Docker or in a local process.

    Feature stage:
    [Alpha](https://github.com/kubeflow/pipelines/blob/master/docs/release/feature-stages.md#alpha)

    In this alpha implementation, we support:
      * Control flow: Condition, ParallelFor
      * Data passing: InputValue, InputPath, OutputPath

    And we don't support:
      * Control flow: ExitHandler, Graph, SubGraph
      * ContainerOp with environment variables, init containers, sidecars, pvolumes
      * ResourceOp
      * VolumeOp
      * Caching

    Args:
      pipeline_func: A function that describes a pipeline by calling components
        and composing them into execution graph.
      arguments: Arguments to the pipeline function provided as a dict.
        reference to `kfp.client.create_run_from_pipeline_func`.
      local_client: Optional. An instance of kfp.LocalClient.
      pipeline_root: Optional. The root directory where the output artifact of component
        will be saved.
      execution_mode: Configuration to decide whether the client executes component
        in docker or in local process.
    """
    local_client = local_client or LocalClient(pipeline_root)
    return local_client.create_run_from_pipeline_func(
        pipeline_func, arguments, execution_mode=execution_mode)
