# Episode 08-autopilot: Scheduling Compute in the Cloud.

**This example revisits 'Episode 06-statistics-redux: Computing in the Cloud'. 
With Metaflow, you don't need to make any code changes to schedule your flow
in the cloud. In this example we will schedule the 'stats.py' workflow
using the 'step-functions create' command line argument. This instructs 
Metaflow to schedule your flow on AWS Step Functions without changing any code. 
You can execute your flow on AWS Step Functions by using the 
'step-functions trigger' command line argument. You can control the behavior 
with additional arguments, like '--max-workers'. For this example, 'max-workers'
is used to limit the number of parallel genre  specific statistics computations.
You can then access the data artifacts (even the local CSV file) from anywhere
because the data is being stored in AWS S3.**

#### Showcasing:
- 'step-functions create' command line option
- 'step-functions trigger' command line option
- Accessing data locally or remotely

#### Before playing this episode:
1. Configure your sandbox: https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox
2. ```python -m pip install pandas```
3. ```python -m pip install notebook```
4. ```python -m pip install matplotlib```

#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python 02-statistics/stats.py step-functions create --max-workers 4```
3. ```python 02-statistics/stats.py step-functions trigger```
3. ```jupyter-notebook 08-autopilot/autopilot.ipynb```
4. Open 'autopilot.ipynb' in your remote Sagemaker notebook
