context("test-parameters.R")

test_that("parameters are formatted correctly", {
  skip_if_no_metaflow()
  metaflow("ParameterFlow") %>%
    parameter("alpha",
      help = "Learning rate",
      default = 0.01
    )
  actual <- ParameterFlow$get_parameters()
  expected <- "    alpha = Parameter('alpha',\n                      help = 'Learning rate',\n                      default = 0.01)\n"
  expect_equal(actual, expected)
  metaflow("TestFlow") %>%
    parameter("num_components",
      help = "Number of components",
      required = TRUE,
      type = "int"
    )
  actual <- TestFlow$get_parameters()
  expected <- "    num_components = Parameter('num_components',\n                               required = True,\n                               help = 'Number of components',\n                               type = int)\n"
  expect_equal(actual, expected)
})


test_that("multiple parameters formatted correctly", {
  skip_if_no_metaflow()
  metaflow("TestFlow") %>%
    parameter("alpha",
      help = "Learning rate",
      default = 0.01
    ) %>%
    parameter("date",
      help = "Date",
      default = "20180101"
    )
  actual <- TestFlow$get_parameters()
  expected <- c(
    "    alpha = Parameter('alpha',\n                      help = 'Learning rate',\n                      default = 0.01)\n",
    "    date = Parameter('date',\n                     help = 'Date',\n                     default = '20180101')\n"
  )
  expect_equal(actual, expected)
})

test_that("parameters work", {
  skip_if_no_metaflow()
  metaflow("TestFlow") %>%
    parameter("country_title_pairs",
      help = "A list of country-title pairs",
    )
  actual <- TestFlow$get_parameters()
  expected <- "    country_title_pairs = Parameter('country_title_pairs',\n                                    help = 'A list of country-title pairs')\n"
  expect_equal(actual, expected)

  metaflow("TestFlow") %>%
    parameter("dry_run",
      help = "Do not write results to a Hive table.",
      is_flag = TRUE,
      default = FALSE
    )
  actual <- TestFlow$get_parameters()
  expected <- "    dry_run = Parameter('dry_run',\n                        help = 'Do not write results to a Hive table.',\n                        default = False,\n                        is_flag = True)\n"
})

test_that("test parameter format", {
  skip_if_no_metaflow()
  actual <- fmt_parameter(parameter_arg = "test", space = 10)
  expected <- c("test", "\n", "          ")
  expect_equal(actual, expected)
  actual <- fmt_parameter(expected, parameter_string = "test", space = 10)
  expected <- c("test", "test", "\n", "          ", "\n", "          ")
  expect_equal(actual, expected)
})
