library(reticulate)

virtualenv_create("r-metaflow")
virtualenv_install("r-metaflow", c("../..", "pandas", "numpy"))
use_virtualenv("r-metaflow")

source("testthat.R")
source("run_integration_tests.R")