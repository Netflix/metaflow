# Welcome to Metaflow R 
Metaflow provides a unified API to the infrastructure stack that is required to execute data science projects, from prototype to production.

Under the hood, Metaflow R uses the [Python version](https://docs.metaflow.org) of Metaflow to power its actions. Hence the R community can benefit from all the battle-hardening and testing that goes into the Python version of Metaflow. Since this is a technical detail, as an R user, you don't have to worry about Python unless you want to.

## What Metaflow offers for R users
We love the data science ecosystem provided by the R community for example the [tidyverse](https://tidyverse.org) package suite, ergonomic [data wrangling tools](https://dplyr.tidyverse.org/), slick interactive communication tools such as [R Shiny](https://shiny.rstudio.com/), a data science oriented IDE [RStudio](https://rstudio.com/), and cutting-edge libraries for statistical computing on [CRAN](https://cran.r-project.org/web/packages/available_packages_by_name.html). 

Metaflow wants to provide a better infrastructure stack for data scientists in R community. Some of the key features of Metaflow are:
Computation graph. Computation is broken down into a DAG where each node is a computation step.
1. **Isolation**. Each step runs in an isolated environment, which can be either a local process, or a remote execution on [AWS Batch](https://aws.amazon.com/batch/). 
2. **Version control with data lineage**. Code and data created in each step are persisted together in each run as immutable Metaflow Artifacts. By doing this, we have built-in data lineage within a flow, which tracks the data origin, what happens to it and where it moves through out the flow.
3. **Data management**. Data read/write paths are automatically maintained since data is persisted in each run step. 
4. **Steps as managed checkpoints**. We can resume from any step in any past run without recomputing from start.
5. **Infrastructure as Code**. We can specify computation resource requirements in native R code.
6. **Tight integration with AWS for easy scalability**. Scale out or schedule production runs in AWS without changing your r functions implementations.
7. **Failure as features**. Robust end-to-end execution.
8. **Collaboration**. Namespace and tags for organizing models and experiments across the team.

## Get in touch
Thank you for your interest in Metaflow. We are here to help. The team is most active on our [Gitter channel](https://chat.metaflow.org). There are several ways to get in touch with us:
1. Open an issue at: https://github.com/Netflix/metaflow 
2. Email us at: help@metaflow.org
3. Chat with us on Gitter: https://chat.metaflow.org