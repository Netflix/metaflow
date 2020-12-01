# Episode mnist: Scheduling Compute on Argo Workflows.

**This example demonstrates a MNIST model training in the Kuberntes cluster
with Argo Workflows installed.  With Metaflow-Argo plugin, you don't need to
make any code changes to schedule your flow on Argo Workflows.
In this example we will schedule the 'mnist-training.py' workflow
using the 'argo create' command line argument. This instructs
Metaflow to schedule your flow on Argo Workflows without changing any code.
It will create the Argo WorkflowTemplate from the Metaflow's flow.
You can execute a workflow from the WorkflowTemplate by using the
'argo trigger' command line argument. You can use the 'argo list-runs'
command to monitor your workflows.**

#### Showcasing:
- 'argo create' command line option
- 'argo trigger' command line option
- 'argo list-runs' command line option

#### Before playing this episode:
1. Configure your Metaflow environment:
https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Configuring-Metaflow-Argo
2. ```python -m pip install tensorflow```

#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python mnist/mnist-training.py argo create```
3. ```python mnist/mnist-training.py argo trigger```
4. ```python mnist/mnist-training.py argo list-runs```
