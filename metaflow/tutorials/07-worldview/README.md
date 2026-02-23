# Episode 07-worldview: Way up here.

**This episode shows how you can use a notebook to setup a simple dashboard to
monitor all of your Metaflow flows.**

#### Showcasing:
- The Metaflow client API.

#### Before playing this episode:
1. ```python -m pip install notebook``` (only locally, if you don't have it already)

2. This tutorial assumes that `HelloCloudFlow` (from Episode 05) has been successfully executed with Kubernetes or cloud configuration. The notebook retrieves runs using:

   ```
   flow = Flow('HelloCloudFlow')
   ```
If you are running Metaflow locally without Kubernetes/cloud configuration, you can modify the notebook to inspect other locally executed flows such as:
- ```Flow('HelloFlow')```
- ```Flow('PlayListFlow')```


#### To play this episode:
1. Open ```07-worldview/worldview.ipynb```
