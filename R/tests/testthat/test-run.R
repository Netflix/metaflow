context("test-run.R")

extract_args <- function(x) {
  x[-1]
}

test_that("test run_argv is correctly passing default flags.", {
  skip_if_no_metaflow()
  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint", "run"
  )
  actual <- run_argv("flow.RDS") %>%
    extract_args()
  expect_equal(actual, expected)
})

test_that("test run_argv correctly parses --with batch", {
  skip_if_no_metaflow()
  actual <- run_argv("flow.RDS", batch = TRUE) %>%
    extract_args()
  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint", "--with",
    "batch", "run"
  )
  expect_equal(actual, expected)
})

test_that("test run_argv correctly parses help", {
  skip_if_no_metaflow()
  actual <- run_argv("flow.RDS", help = TRUE) %>%
    extract_args()
  expected <- c("--flowRDS=flow.RDS", "--no-pylint", "--help")
  expect_equal(actual, expected)
})

test_that("test run_argv preserves shell-sensitive values", {
  skip_if_no_metaflow()
  dangerous_value <- "spaces quotes \" ' semicolon; dollar$HOME"
  unicode_value <- "unicode \u2603"
  expected <- c(
    "--flowRDS=flow file.RDS",
    "--no-pylint", "run",
    "--tag", dangerous_value,
    "--tag", unicode_value,
    "--name", dangerous_value,
    "--extra", dangerous_value,
    "--unicode", unicode_value
  )
  actual <- run_argv(
    "flow file.RDS",
    tag = c(dangerous_value, unicode_value),
    name = dangerous_value,
    other_args = c("--extra", dangerous_value, "--unicode", unicode_value)
  ) %>%
    extract_args()
  expect_equal(actual, expected)
})

test_that("test run_argv preserves batch identifiers as single arguments", {
  skip_if_no_metaflow()
  run_id <- "run id; still one arg"
  user <- "user quotes \" ' \u2603"
  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint",
    "batch", "status",
    paste0("--run-id=", run_id),
    paste0("--user=", user)
  )
  actual <- run_argv(
    "flow.RDS",
    batch = "status",
    run_id = run_id,
    user = user
  ) %>%
    extract_args()
  expect_equal(actual, expected)
})

test_that("test run_cmd quotes shell-sensitive values", {
  skip_if_no_metaflow()
  dangerous_value <- "spaces quotes \" ' semicolon; dollar$HOME"
  actual <- run_cmd("flow file.RDS", tag = dangerous_value)
  expect_match(actual, shQuote("--flowRDS=flow file.RDS"), fixed = TRUE)
  expect_match(actual, shQuote(dangerous_value), fixed = TRUE)
})
