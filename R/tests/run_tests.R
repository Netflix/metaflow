library(reticulate)

virtualenv_create("r-metaflow")
virtualenv_install("r-metaflow", c("../..", "pandas", "numpy"))
print('finish venv install')
use_virtualenv("r-metaflow")
print("using r-metaflow")
#python_bin <- py_discover_config("metaflow", "r-metaflow")$python
#print(python_bin)
#Sys.setenv("METAFLOW_PYTHON" = python_bin)

source("testthat.R")
source("run_integration_tests.R")