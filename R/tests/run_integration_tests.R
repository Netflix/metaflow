if (!require(data.table)) {
  install.packages("data.table", repos = "https://cloud.r-project.org")
}
if (!require(Matrix)) {
  install.packages("Matrix", repos = "https://cloud.r-project.org")
}
if (!require(glmnet)) {
  install.packages("glmnet", repos = "https://cloud.r-project.org")
}
if (!require(caret)) {
  install.packages("caret", repos = "https://cloud.r-project.org")
}
if (!require(caret)) {
  install.packages("rjson", repos = "https://cloud.r-project.org")
}

library(rjson)
source("formatter.R")
source("utils.R")

run_tests <- function(context) {
  graph_files <- list.files(path = "./graphs", pattern = "\\.json$", full.names = TRUE)
  test_files <- list.files(path = "./tests", pattern = "\\.R$", full.names = TRUE)

  for (graph_fname in graph_files) {
    for (test_fname in test_files) {
      source(test_fname)
      graphspec <- fromJSON(file = graph_fname)

      test_flow_file <- "test_flow.R"
      check_flow_file <- "check_flow.R"

      mismatch <- FALSE
      test_flow_lines <- tryCatch(
        {
          flow_lines(graphspec, test, context)
        },
        error = function(e) {
          print(e)
          cat(sprintf(
            "Skipping test %s with graph %s.\n",
            test$name, graphspec$name
          ))
          mismatch <<- TRUE
        }
      )

      if (mismatch) {
        next
      }

      writeLines(test_flow_lines, con = test_flow_file)
      writeLines(check_lines(test), con = check_flow_file)

      source(test_flow_file)
      stopifnot(status_code == 0)

      source(check_flow_file)
      cat(sprintf("%sFlow passed test with graph %s\n", test$name, graphspec$name))
    }
  }
}

run_tests_all_contexts <- function() {
  contexts <- fromJSON(file = "./contexts.json")

  for (context in contexts$contexts) {
    if (!context$disabled) {
      run_tests(context)
    }
  }
}

run_tests_all_contexts()
