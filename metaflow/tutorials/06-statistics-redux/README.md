# Episode 06-statistics-redux: Computing in the Cloud.

**This example revisits 'Episode 02-statistics: Is this Data Science?'. With
Metaflow, you don't need to make any code changes to scale-up your flow by
running on remote compute. In this example we re-run the 'stats.py' workflow
adding the '--with batch' command line argument. This instructs Metaflow to run
all your steps on AWS batch without changing any code. You can control the
behavior with additional arguments, like '--max-workers'. For this example,
'max-workers' is used to limit the number of parallel genre specific statistics
computations.
You can then access the data artifacts (even the local CSV file) from anywhere
because the data is being stored in AWS S3.
This tutorial uses `pandas` which may not be available in your environment. 
Use the 'conda' package manager with the `conda-forge` channel added to run 
this tutorial in any environment**

#### Showcasing:
- '--with batch' command line option
- '--max-workers' command line option
- Accessing data locally or remotely
- Metaflow's conda based dependency management.


#### Before playing this episode:
1. ```python -m pip install pandas```
2. ```python -m pip install notebook```
3. ```python -m pip install matplotlib```
4. This tutorial requires the 'conda' package manager to be installed with the
   conda-forge channel added.
   a. Download Miniconda at https://docs.conda.io/en/latest/miniconda.html
   b. ```conda config --add channels conda-forge```
5. This tutorial requires access to compute and storage resources on AWS, which
   can be configured by 
   a. Following the instructions at 
      https://docs.metaflow.org/metaflow-on-aws/deploy-to-aws or
   b. Requesting a sandbox at 
      https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox


#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python 02-statistics/stats.py --environment conda run --with batch --max-workers 4 --with conda:python=3.7,libraries="{pandas:0.24.2}"```
3. ```jupyter-notebook 06-statistics-redux/stats.ipynb```
4. Open 'stats.ipynb' in your remote Sagemaker notebook
