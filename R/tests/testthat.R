library(testthat)
library(reticulate)

virtualenv_create("metaflow-r")
virtualenv_install("metaflow-r", "../..")
use_virtualenv("metaflow-r")

test_check("metaflow")
