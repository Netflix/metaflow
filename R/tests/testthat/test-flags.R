context("test-flags.R")

arguments <- c("--alpha 100", "--with catch", "--with retry")
parameter_arguments <- c("--alpha 100", "--date 20190101")

test_that("split_flags", {
  skip_if_no_metaflow()
  expected <- lapply(arguments, function(x) {
    strsplit(x, split = " ")
  }) %>%
    unlist()
  actual <- split_flags(arguments)
  expect_equal(actual, expected)
})

test_that("parse --help", {
  skip_if_no_metaflow()
  actual <- parse_arguments("--help")
  expected <- list(help = TRUE)
  expect_equal(actual, expected)
})

test_that("parse arguments from R", {
  skip_if_no_metaflow()
  actual <- parse_arguments(arguments)
  expected <- list(
    alpha = "100",
    with = c("catch", "retry")
  )
  expect_equal(actual, expected)
})

test_that("parse arguments from command line", {
  skip_if_no_metaflow()
  system2(
    "Rscript",
    args = c("test-command-args.R", "--alpha", "100", "--with", "catch", "--with", "retry")
  )
  actual <- readRDS("flags.RDS")
  message(actual)
  expected <- list(
    alpha = "100",
    with = c("catch", "retry")
  )
  expect_equal(actual, expected)
  on.exit(file.remove("flags.RDS"))
})

test_that("split parameters sets valid params", {
  skip_if_no_metaflow()
  arguments <- split_flags(parameter_arguments) %>%
    parse_arguments()
  actual <- split_parameters(arguments)
  expected <- "--alpha 100 --date 20190101"
  expect_equal(actual, expected)
  flags <- flags()
  actual <- split_parameters(flags)
  expected <- ""
  expect_equal(actual, expected)
})

test_that("split parameter args preserves shell-sensitive values", {
  skip_if_no_metaflow()
  arguments <- list(
    alpha = "has spaces; and semicolon",
    name = "quotes \" '",
    label = "unicode \u2603"
  )
  actual <- split_parameter_args(arguments)
  expected <- c(
    "--alpha", "has spaces; and semicolon",
    "--name", "quotes \" '",
    "--label", "unicode \u2603"
  )
  expect_equal(actual, expected)
})

test_that("split parameter args repeats vector parameter flags", {
  skip_if_no_metaflow()
  arguments <- list(alpha = c("one", "two"))
  actual <- split_parameter_args(arguments)
  expected <- c("--alpha", "one", "--alpha", "two")
  expect_equal(actual, expected)
})

test_that("resume functionality works", {
  skip_if_no_metaflow()
  actual <- parse_arguments(list("resume", "--alpha=100"))
  expected <- list(
    resume = TRUE,
    alpha = "100"
  )
  expect_equal(actual, expected)
})
