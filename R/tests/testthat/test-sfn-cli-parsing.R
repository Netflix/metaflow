read_run_argv <- function(...) {
  on.exit(if (file.exists("run_cmd.RDS")) file.remove("run_cmd.RDS"), add = TRUE)
  system2("Rscript", args = c("test-run-cmd.R", ...))
  run_argv <- readRDS("run_cmd.RDS")
  run_argv[-1]
}

test_that("SFN create", {
  skip_if_no_metaflow()

  actual <- read_run_argv("step-functions", "create")

  expected <- c("--flowRDS=flow.RDS", "--no-pylint", "step-functions", "create")

  expect_equal(actual, expected)
})

test_that("SFN create --help", {
  skip_if_no_metaflow()

  actual <- read_run_argv("step-functions", "create", "--help")

  expected <- c("--flowRDS=flow.RDS", "--no-pylint", "step-functions", "create", "--help")

  expect_equal(actual, expected)
})

test_that("SFN create --package-suffixes", {
  skip_if_no_metaflow()

  actual <- read_run_argv("--package-suffixes=.csv,.RDS,.R", "step-functions", "create")

  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint",
    "--package-suffixes=.csv,.RDS,.R",
    "step-functions",
    "create"
  )

  expect_equal(actual, expected)
})

test_that("SFN create --generate-new-token", {
  skip_if_no_metaflow()

  actual <- read_run_argv("step-functions", "create", "--generate-new-token")

  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint",
    "step-functions",
    "create",
    "--generate-new-token"
  )

  expect_equal(actual, expected)
})

test_that("SFN create --generate-new-token --max-workers 100 --lr 0.01", {
  skip_if_no_metaflow()

  actual <- read_run_argv(
    "step-functions", "create", "--generate-new-token",
    "--max-workers", "100", "--lr", "0.01"
  )

  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint",
    "step-functions",
    "create",
    "--generate-new-token",
    "--max-workers", "100",
    "--lr", "0.01"
  )
  expect_equal(actual, expected)
})


test_that("SFN trigger", {
  skip_if_no_metaflow()

  actual <- read_run_argv("step-functions", "trigger")

  expected <- c("--flowRDS=flow.RDS", "--no-pylint", "step-functions", "trigger")

  expect_equal(actual, expected)
})


test_that("SFN list-runs --running", {
  skip_if_no_metaflow()

  actual <- read_run_argv("step-functions", "list-runs", "--running")

  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint",
    "step-functions",
    "list-runs",
    "--running"
  )

  expect_equal(actual, expected)
})
