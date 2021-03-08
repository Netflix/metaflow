# Episode 04-helloaws: Look Mom, We're in the Cloud.

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
1. Configure your sandbox: https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox

#### To play this episode:
##### Execute the flow:
In a terminal:
1. ```cd tutorials/04-helloaws```
2. ```Rscript helloaws.R run```

If you are using RStudio, you can run this script by directly executing `source("helloaws.R")`.

##### Inspect the results:
Open the R Markdown file ```helloaws.Rmd``` in RStudio and execute the markdown cells.