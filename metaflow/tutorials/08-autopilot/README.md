# Episode 08-autopilot: Scheduling Compute in the Cloud.

**This example revisits 'Episode 06-statistics-redux: Computing in the Cloud'.
With Metaflow, you don't need to make any code changes to schedule your flow
in the cloud. In this example we will schedule the 'stats.py' workflow
using the 'argo-workflows create' command line argument. This instructs
Metaflow to schedule your flow on Argo Workflows without changing any code.
You can execute your flow on Argo Workflows by using the
'step-functions trigger' command line argument. You can use a notebook to setup
a simple dashboard to monitor all of your Metaflow flows.**

#### Showcasing:
- 'argo-workflows create' command line option
- 'argo-workflows trigger' command line option
- Accessing data locally or remotely through the Metaflow Client API

#### To play this episode:
1. ```python 02-statistics/stats.py --with kubernetes argo-workflows create --max-workers 4```
2. ```python 02-statistics/stats.py argo-workflows trigger ```
3. Open ```08-autopilot/autopilot.ipynb```
