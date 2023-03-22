# Episode 06-statistics-redux: Computing in the Cloud.

**This example revisits 'Episode 02-statistics: Is this Data Science?'. With
Metaflow, you don't need to make any code changes to scale-up your flow by
running on remote compute. In this example we re-run the 'stats.py' workflow
adding the '--with kubernetes' command line argument. This instructs Metaflow to run
all your steps on AWS Kubernetes without changing any code. You can control the
behavior with additional arguments, like '--max-workers'. For this example,
'max-workers' is used to limit the number of parallel genre-specific statistics
computations.
You can then access the data artifacts (even the local CSV file) from anywhere
because the data is being stored in AWS S3.**

#### Showcasing:
- '--with kubernetes' command line option
- '--max-workers' command line option
- Accessing data locally or remotely

#### To play this episode:
1. ```python 02-statistics/stats.py run --with kubernetes --max-workers 4```
2. Open ```06-statistics-redux/stats.ipynb```