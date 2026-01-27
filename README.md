![Metaflow_Logo_Horizontal_FullColor_Ribbon_Dark_RGB](https://user-images.githubusercontent.com/763451/89453116-96a57e00-d713-11ea-9fa6-82b29d4d6eff.png)

# Metaflow

[Metaflow](https://metaflow.org) is a human-centric framework designed to help scientists and engineers **build and manage real-life AI and ML systems**. Serving teams of all sizes and scale, Metaflow streamlines the entire development lifecycle—from rapid prototyping in notebooks to reliable, maintainable production deployments—enabling teams to iterate quickly and deliver robust systems efficiently.

Originally developed at [Netflix](https://netflixtechblog.com/open-sourcing-metaflow-a-human-centric-framework-for-data-science-fa72e04a5d9) and now supported by [Outerbounds](https://outerbounds.com), Metaflow is designed to boost the productivity for research and engineering teams working on [a wide variety of projects](https://netflixtechblog.com/supporting-diverse-ml-systems-at-netflix-2d2e6b6d205d), from classical statistics to state-of-the-art deep learning and foundation models. By unifying code, data, and compute at every stage, Metaflow ensures seamless, end-to-end management of real-world AI and ML systems.

Today, Metaflow powers thousands of AI and ML experiences across a diverse array of companies, large and small, including Amazon, Doordash, Dyson, Goldman Sachs, Ramp, and [many others](ADOPTERS.md). At Netflix alone, Metaflow supports over 3000 AI and ML projects, executes hundreds of millions of data-intensive high-performance compute jobs processing petabytes of data and manages tens of petabytes of models and artifacts for hundreds of users across its AI, ML, data science, and engineering teams.

## From prototype to production (and back)

Metaflow provides a simple and friendly pythonic [API](https://docs.metaflow.org) that covers foundational needs of AI and ML systems:
<img src="./docs/prototype-to-prod.png" width="800px">

1. [Rapid local prototyping](https://docs.metaflow.org/metaflow/basics), [support for notebooks](https://docs.metaflow.org/metaflow/managing-flows/notebook-runs), and built-in support for [experiment tracking, versioning](https://docs.metaflow.org/metaflow/client) and [visualization](https://docs.metaflow.org/metaflow/visualizing-results).
2. [Effortlessly scale horizontally and vertically in your cloud](https://docs.metaflow.org/scaling/remote-tasks/introduction), utilizing both CPUs and GPUs, with [fast data access](https://docs.metaflow.org/scaling/data) for running [massive embarrassingly parallel](https://docs.metaflow.org/metaflow/basics#foreach) as well as [gang-scheduled](https://docs.metaflow.org/scaling/remote-tasks/distributed-computing) compute workloads [reliably](https://docs.metaflow.org/scaling/failures) and [efficiently](https://docs.metaflow.org/scaling/checkpoint/introduction).
3. [Easily manage dependencies](https://docs.metaflow.org/scaling/dependencies) and [deploy with one-click](https://docs.metaflow.org/production/introduction) to highly available production orchestrators with built in support for [reactive orchestration](https://docs.metaflow.org/production/event-triggering).

For full documentation, check out our [API Reference](https://docs.metaflow.org/api) or see our [Release Notes](https://github.com/Netflix/metaflow/releases) for the latest features and improvements. 


## Getting started

Getting up and running is easy. If you don't know where to start, [Metaflow sandbox](https://outerbounds.com/sandbox) will have you running and exploring in seconds.

### Installing Metaflow

To install Metaflow in your Python environment from [PyPI](https://pypi.org/project/metaflow/):

```sh
pip install metaflow
```
Alternatively, using [conda-forge](https://anaconda.org/conda-forge/metaflow):

```sh
conda install -c conda-forge metaflow
```

Once installed, a great way to get started is by following our [tutorial](https://docs.metaflow.org/getting-started/tutorials). It walks you through creating and running your first Metaflow flow step by step.  

For more details on Metaflow’s features and best practices, check out:
- [How Metaflow works](https://docs.metaflow.org/metaflow/basics)  
- [Additional resources](https://docs.metaflow.org/introduction/metaflow-resources)  

If you need help, don’t hesitate to reach out on our [Slack community](http://slack.outerbounds.co/)!


### Deploying infrastructure for Metaflow in your cloud
<img src="./docs/multicloud.png" width="800px">


While you can get started with Metaflow easily on your laptop, the main benefits of Metaflow lie in its ability to [scale out to external compute clusters](https://docs.metaflow.org/scaling/remote-tasks/introduction) 
and to [deploy to production-grade workflow orchestrators](https://docs.metaflow.org/production/introduction). To benefit from these features, follow this [guide](https://outerbounds.com/engineering/welcome/) to 
configure Metaflow and the infrastructure behind it appropriately.


## Get in touch
We'd love to hear from you. Join our community [Slack workspace](http://slack.outerbounds.co/)!

## Contributing
We welcome contributions to Metaflow. Please see our [contribution guide](https://docs.metaflow.org/introduction/contributing-to-metaflow) for more details.
