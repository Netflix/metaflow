# Episode 05-hellocloud: Look Mom, We're in the Cloud.

**This flow is a simple linear workflow that verifies your Kubernetes
configuration. The 'start' and 'end' steps will run locally, while the 'hello'
step will run remotely on Kubernetes. After configuring Metaflow to run on the cloud,
data and metadata about your runs will be stored remotely. This means you can
use the client to access information about any flow from anywhere.**

#### Showcasing:
- Kubernetes decorator.
- Accessing data artifacts generated remotely in a local notebook.
- retry decorator.

#### To play this episode:
Open ```05-hello-cloud/hello-cloud.ipynb```

# Episode 5: Hello Cloud

## Look Mom, We're in the Cloud.

This flow is a simple linear workflow that verifies your cloud configuration. The `start` and `end` steps will run locally, while the `hello` step will [run remotely](/scaling/remote-tasks/introduction). After [configuring Metaflow](/getting-started/infrastructure) to run in the cloud, data and metadata about your runs will be stored remotely. This means you can use the client to access information about any flow from anywhere.

You can find the tutorial code on [GitHub](https://github.com/Netflix/metaflow/tree/master/metaflow/tutorials/05-hello-cloud)

**Showcasing:**

- [Kubernetes](https://docs.metaflow.org/scaling/remote-tasks/kubernetes) and the [`@kubernetes`](https://docs.metaflow.org/scaling/remote-tasks/introduction) decorator.
- Using the [Client API](../../../metaflow/client) to access data artifacts generated remotely in a local notebook.
- [`@retry`](https://docs.metaflow.org/scaling/failures#retrying-tasks-with-the-retry-decorator)decorator.

**Before playing this episode:**

1. `python -m pip install notebook`
2. This tutorial requires access to compute and storage resources in the cloud, which can be configured by
   1. Following the instructions [here](https://outerbounds.com/docs/engineering-welcome/) or
   2. Requesting [a sandbox](https://outerbounds.com/sandbox/).

**To play this episode:**

1. `cd metaflow-tutorials`
2. `python 05-hello-cloud/hello-cloud.py run`
3. `jupyter-notebook 05-hello-cloud/hello-cloud.ipynb`
4. Open _**hello-cloud.ipynb**_ in a notebook

<TutorialsLink link="../../tutorials"/>