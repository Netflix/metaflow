library(reticulate)

virtualenv_create("metaflow-test")
virtualenv_install("metaflow-test", c("../..", "pandas", "numpy"))
use_virtualenv("metaflow-test")
python_bin <- py_discover_config("metaflow", "metaflow-test")$python
Sys.setenv("METAFLOW_PYTHON" = python_bin)

source("testthat.R")
source("run_integration_tests.R")

