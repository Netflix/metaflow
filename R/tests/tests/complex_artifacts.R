test <- new.env()
test$name <- "ComplexArtifactsTest"
test$priority <- 1

test$single <- decorated_function(
  function(self) {
    if (!suppressWarnings(require(data.table))) {
      install.packages("data.table", quiet = TRUE, repos = "https://cloud.r-project.org/")
    }
    if (!suppressWarnings(require(Matrix))) {
      install.packages("Matrix", quiet = TRUE, repos = "https://cloud.r-project.org/")
    }
    if (!suppressWarnings(require(glmnet, war))) {
      install.packages("glmnet", quiet = TRUE, repos = "https://cloud.r-project.org/")
    }

    self$special <- c(NaN, Inf)

    self$nested_list <- list(
      a = c(1, 3, 5),
      list(b = c(2, 8, 6), c = c("a", "b", "c"))
    )

    suppressPackageStartupMessages(library(data.table))
    self$dt <- data.table(
      ID = c("b", "b", "b", "a", "a", "c"),
      a = 1:6,
      b = 7:12,
      c = 13:18
    )

    suppressPackageStartupMessages(library(Matrix))
    self$matrix <- Matrix(10 + 1:28, 4, 7)

    suppressPackageStartupMessages(library(glmnet))
    set.seed(2020)
    x <- matrix(rnorm(100 * 20), 100, 20)
    y <- rnorm(100)
    fit <- glmnet(x, y)
    self$fit <- fit
  },
  type = "step", prio = 0, qual = c("singleton"), required = TRUE
)

test$end <- decorated_function(
  function(self) {
    stopifnot(is.nan(self$special[1]))
    stopifnot(is.infinite(self$special[2]))

    stopifnot(self$nested_list$b[[2]] == 8)

    stopifnot(self$dt$b[3] == 9)
    stopifnot(self$dt$ID[4] == "a")

    stopifnot(sum(self$matrix) == 686)

    stopifnot(sum(which(self$fit$beta[, 2] != 0)) == 14)
    stopifnot(sum(which(self$fit$beta[, 17] != 0)) == 119)
  },
  type = "step", prio = 0, qual = c("end"), required = TRUE
)

test$all <- decorated_function(
  function(self) {
  },
  type = "step", prio = 1, qual = c("all")
)
