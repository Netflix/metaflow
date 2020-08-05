# Episode 05-statistics-redux: Computing in the Cloud.

**This example revisits 'Episode 02-statistics: Is this Data Science?'. With
Metaflow, you don't need to make any code changes to scale-up your flow by
running on remote compute. In this example we re-run the 'stats.py' workflow
adding the '--with batch' command line argument. This instructs Metaflow to run
all your steps on AWS batch without changing any code. You can control the
behavior with additional arguments, like '--max-workers'. For this example,
'max-workers' is used to limit the number of parallel genre specific statistics
computations.
You can then access the data artifacts (even the local CSV file) from anywhere
because the data is being stored in AWS S3.**

#### Showcasing:
- ```--with batch``` command line option
- ```--max-workers``` command line option
- Accessing data artifact stored in AWS S3 from a local Markdown Notebook.

#### Before playing this episode:
1. Configure your sandbox: https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox

#### To play this episode:
1. ```cd metaflow-tutorials/R/02-statistics/```
2. ```Rscript stats.R --package-suffixes=.R,.csv run --with batch --max-workers 4```
3. Open ```02-statistics/stats.Rmd``` in your RStudio and re-run the cells. You can acccess
the artifacts stored in AWS S3 from your local RStudio session. 
