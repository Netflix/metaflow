<!-- ![Metaflow_Logo_Horizontal_FullColor_Ribbon_Dark_RGB](https://user-images.githubusercontent.com/763451/89453116-96a57e00-d713-11ea-9fa6-82b29d4d6eff.png) -->

<img src="./docs/metaflow.svg" width="800px">

Metaflow is a human-friendly Python library that helps scientists and engineers build and manage real-life data science projects. Metaflow was [originally developed at Netflix](https://netflixtechblog.com/open-sourcing-metaflow-a-human-centric-framework-for-data-science-fa72e04a5d9) to boost productivity of data scientists who work on a wide variety of projects from classical statistics to state-of-the-art deep learning.

Here is a link to the [official documentation](https://docs.metaflow.org).

## From prototype to production (and back)

Metaflow provides a simple, friendly API that covers foundational needs of ML, AI, and data science projects:
<img src="./docs/prototype-to-prod.png" width="800px">

1. Rapid local prototyping, support for notebooks, and built-in experiment tracking and versioning.
2. Horizontal and vertical scalability to the cloud, utilizing both CPUs and GPUs, and fast data access.
3. One-click deployments to highly available production orchestrators.


## Getting started

Getting up and running is easy. If you don't know where to start, click the link to get started in your [Metaflow sandbox](https://outerbounds.com/sandbox), where you can run code and explore Metaflow in seconds.

### Installing Metaflow in your Python environment

To install Metaflow into your local environment, you can install from [PyPi](https://pypi.org/project/metaflow/):

```sh
pip install metaflow
```
Alternatively, you can install from [Conda](https://anaconda.org/conda-forge/metaflow):

```sh
conda install -c conda-forge metaflow
```

## Resources

### [Slack](http://slack.outerbounds.co/)
An active community of data scientists and ML engineers discussing the ins-and-outs of applied machine learning. Get answers to your MLOps questions in minutes.

### [Tutorials](https://outerbounds.com/docs/tutorials-index/)
Demonstrations of Metaflow in real-world data science contexts. Beginner tutorials are linked below, and the budding Metaflow power user can find more advanced content by exploring [here](https://outerbounds.com/docs/tutorials-index/).
- [Introduction to Metaflow](https://outerbounds.com/docs/intro-tutorial-overview/)
- [Natural Language Processing with Metaflow](https://outerbounds.com/docs/nlp-tutorial-overview/)
- [Computer Vision with Metaflow](https://outerbounds.com/docs/cv-tutorial-overview/)
- [Recommender Systems with Metaflow](https://outerbounds.com/docs/recsys-tutorial-overview/)

### [Generative AI and LLM use cases](https://outerbounds.com/blog/?category=Foundation%20Models)
- [Parallelizing Stable Diffusion for Production Use Cases](https://outerbounds.com/blog/parallelizing-stable-diffusion-production-use-cases/)
- [Whisper with Metaflow on Kubernetes](https://outerbounds.com/blog/whisper-kubernetes/)
- [Training a Large Language Model With Metaflow, Featuring Dolly](https://outerbounds.com/blog/train-dolly-metaflow/)

### [Guides for data scientists](https://outerbounds.com/docs/data-science-welcome/)
Short posts written to unblock data scientists without breaking their focus. Check [here](https://outerbounds.com/docs/data-science-welcome/) for answers to common Metaflow questions written in a Stack Overflow Q&A style.

### Case studies and best practices

This section is a sample of Metaflow content that will not be updated at high frequency. To find the latest Metaflow case studies and best practices, head over to the [Outerbounds blog](https://outerbounds.com/blog/).

| Post title | Patterns | Complimentary tools |
| ---------- | -------- | ----- |
| [Fast Data: Loading Tables From S3 At Lightning Speed](https://outerbounds.com/blog/metaflow-fast-data/) | Optimizing I/O for data transfer in ML systems | Apache Arrow |
| [Case Study: MLOps for FinTech using Metaflow](https://outerbounds.com/blog/mlops-fin-tech/) | Versioning, CI/CD, and Why Metaflow? | Snowflake, TensorFlow, MLFlow, Optuna, Arize |
| [Scaling Media ML at Netflix](https://netflixtechblog.com/scaling-media-machine-learning-at-netflix-f19b400243) | Feature store, Multi-GPU, Model Serving, Vector Search | Ray |
| [Better Airflow with Metaflow](https://outerbounds.com/blog/better-airflow-with-metaflow/) | ML Orchestration, Data Engineering Workflows | Airflow |
| [Accelerating ML at CNN](https://medium.com/cnn-digital/accelerating-ml-within-cnn-983f6b7bd2eb) | ML experimentation platform | AWS Batch, Terraform | 
| [Developing Safe and Reliable ML products at 23andMe](https://medium.com/23andme-engineering/machine-learning-eeee69d40736) | Privacy, compliance, data security, and testing ML systems | MLFlow, Jenkins, AWS Fargate |
| [Case Study: MLOps for NLP-powered Media Intelligence using Metaflow](https://outerbounds.com/blog/mlops-media-intelligence/) | Testing, optimizing, and deploying ML workflows | HuggingFace, Neptune, Gradio, PyTorch, ONNX |
| [Machine Learning with the Modern Data Stack: A Case Study](https://outerbounds.com/blog/modern-data-stack-mlops/) | End-to-end ML with Python and SQL | dbt, Snowflake, CometML, Sagemaker | 

### [Metaflow Deployment Guides](https://outerbounds.com/engineering/welcome/)
<img src="./docs/multicloud.png" width="800px">

To set up and operate the full stack of ML/data science infrastructure for Metaflow on your own infrastructure, see these [guides for engineers](https://outerbounds.com/engineering/welcome/). This link will help you navigate the decision for deploying Metaflow in your organization's cloud accounts. 

If you are looking for a managed Metaflow offering, see the [Outerbounds platform](https://outerbounds.com/platform/).

### [Effective Data Science Infrastructure: How to make data scientists productive](https://www.manning.com/books/effective-data-science-infrastructure)
A book about data science infrastructure, with many Metaflow tales included! You can find all code examples in [this repository](https://github.com/outerbounds/dsbook). 

### Get in touch
There are several ways to get in touch with us:
- [Slack](http://slack.outerbounds.co/)
- Open an issue at: https://github.com/Netflix/metaflow 

## Contributing

We welcome contributions to Metaflow. Please see our [contribution guide](https://docs.metaflow.org/introduction/contributing-to-metaflow) for more details.
