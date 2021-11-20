# Episode 05-helloaws: Look Mom, We're in the Cloud.

**This flow is a simple linear workflow that verifies your AWS
configuration. The 'start' and 'end' steps will run locally, while the 'hello'
step will run remotely on AWS batch. After configuring Metaflow to run on AWS,
data and metadata about your runs will be stored remotely. This means you can
use the client to access information about any flow from anywhere.**

#### Showcasing:
- AWS batch decorator.
- Accessing data artifacts generated remotely in a local notebook.
- retry decorator.

#### Before playing this episode:
1. ```python -m pip install notebook```
2. This tutorial requires access to compute and storage resources on AWS, which
   can be configured by 
   a. Following the instructions at 
      https://docs.metaflow.org/metaflow-on-aws/deploy-to-aws or
   b. Requesting a sandbox at 
      https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox

#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python 05-helloaws/helloaws.py run```
3. ```jupyter-notebook 05-helloaws/helloaws.ipynb```
4. Open 'helloaws.ipynb' in your remote Sagemaker notebook
