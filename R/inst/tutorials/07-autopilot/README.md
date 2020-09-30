# Episode 08-autopilot: Scheduling Compute in the Cloud.

**This example revisits 'Episode 05-statistics-redux: Computing in the Cloud'. 
With Metaflow, you don't need to make any code changes to schedule your flow
in the cloud. In this example we will schedule the 'stats.R' workflow
using the 'step-functions create' command line argument. This instructs 
Metaflow to schedule your flow on AWS Step Functions without changing any code. 
You can execute your flow on AWS Step Functions by using the 
'step-functions trigger' command line argument. You can use a notebook to setup
a simple dashboard to monitor all of your Metaflow flows.**

#### Showcasing:
- 'step-functions create' command line option
- 'step-functions trigger' command line option
- Accessing data locally or remotely through the Metaflow Client API

#### Before playing this episode:
1. Configure your sandbox: https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox

#### To play this episode:
1. ```cd tutorials/02-statistics/```
2. ```Rscript stats.R --package-suffixes=.R,.csv step-functions create --max-workers 4```
3. ```Rscript stats.R --package-suffixes=.R,.csv step-functions trigger```
4. Open ```07-autopilot/stats.Rmd``` in your RStudio and re-run the cells. You can acccess
the artifacts stored in AWS S3 from your local RStudio session. 