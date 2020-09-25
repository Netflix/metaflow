context("test-run.R")

extract_args <- function(x) {
  args <- strsplit(x, " ")[[1]][-c(1:2)]
  args[args != ""]
}

test_that("test run_cmd is correctly passing default flags.", {
  skip_if_no_metaflow()
  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint", "run"
  )
  actual <- run_cmd("flow.RDS") %>%
    as.character() %>%
    extract_args()
  expect_equal(actual, expected)
})

test_that("test run_cmd correctly parses --with batch", {
  skip_if_no_metaflow()
  actual <- run_cmd("flow.RDS", batch = TRUE) %>%
    as.character() %>%
    extract_args()
  expected <- c(
    "--flowRDS=flow.RDS",
    "--no-pylint", "--with",
    "batch", "run"
  )
  expect_equal(actual, expected)
})

test_that("test run_cmd correctly parses help", {
  skip_if_no_metaflow()
  actual <- run_cmd("flow.RDS", help = TRUE) %>%
    as.character() %>%
    extract_args()
  expected <- c("--flowRDS=flow.RDS", "--no-pylint", "--help")
  expect_equal(actual, expected)
})
