install_deps <- function(){
  if (!require(caret)){
    install.packages("caret", repos="https://cloud.r-project.org")
  }
  if (!require(gbm)){
    install.packages("gbm", repos="https://cloud.r-project.org")
  }
  if (!require(data.table)){
    install.packages("data.table", repos="https://cloud.r-project.org")
  }
}

load_training_data <- function(self) {
  source("house-price-prediction-functions.R")
  install_deps()

  suppressPackageStartupMessages(library(data.table))
  self$data <- fread("./resources/sample-house-data.csv") 
}

clean_data_set <- function(self) {
  source("house-price-prediction-functions.R")
  install_deps()

  suppressPackageStartupMessages(library(data.table))
  data <- self$data %>% data.table()
  data[, c("date", "id") := NULL]
  char_cols <- names(data)[(sapply(data, class) == "character")]
  for (col in char_cols) set(data, j = col, value = factor(data[[col]]))

  self$labels <- data[, price] 
  self$features <- data[, !"price", with = FALSE] 
}

parameter_grid <- function(self) {
  source("house-price-prediction-functions.R")
  install_deps()

  parameters <- list(
    n.trees = 100,
    shrinkage = .01,
    interaction.depth = 5,
    n.minobsinnode = 1:2
  )
  parameter_grid <- expand.grid(parameters) %>%
    split(1:nrow(.))
  names(parameter_grid) <- NULL
  self$parameters <- parameter_grid
}

fit_models <- function(self) {
  source("house-price-prediction-functions.R")
  install_deps()

  suppressPackageStartupMessages(library(caret))
  param <- self$input
  train_control <- trainControl(
    method = "cv",
    number = 5
  )
  grid <- data.frame(
    interaction.depth = param$interaction.depth,
    shrinkage = param$shrinkage,
    n.trees = param$n.trees,
    n.minobsinnode = param$n.minobsinnode
  )
  x <- self$features 
    
  y <- self$labels 
  gbmfit <- train(
    x = x,
    y = y,
    method = "gbm",
    tuneGrid = grid,
    trControl = train_control,
    verbose = FALSE
  )
  self$model <- gbmfit$finalModel
  self$fit <- gbmfit$results 
  print(self$fit)
}

join <- function(self, inputs) {
  source("house-price-prediction-functions.R")
  install_deps()

  suppressPackageStartupMessages(library(data.table))
  suppressPackageStartupMessages(library(caret))

  fits <- gather_inputs(inputs, "fit") 
  fits <- rbindlist(fits)
  self$fits <- fits
  self$models <- gather_inputs(inputs, "model")
}

select_best_fit <- function(self) {
  source("house-price-prediction-functions.R")
  install_deps()

  suppressPackageStartupMessages(library(data.table))
  suppressPackageStartupMessages(library(caret))
  fits <- self$fits 
  models <- self$models
  best_fit_idx <- which.min(fits$RMSE)
  best_fit_model <- models[[best_fit_idx]]
  self$best_fit <- best_fit_model 
}

score_data <- function(self) {
  source("house-price-prediction-functions.R")
  install_deps()

  suppressPackageStartupMessages(library(data.table))
  suppressPackageStartupMessages(library(caret))
  suppressPackageStartupMessages(library(gbm))
  score_data <- fread("./resources/house_price_scoring_input.csv")
  char_cols <- names(score_data)[(sapply(score_data, class) == "character")]
  for (col in char_cols) set(score_data, j = col, value = factor(score_data[[col]]))
  best_fit <- self$best_fit
  class(best_fit) <- "gbm"
  predictions <- predict(best_fit, score_data, n.trees = best_fit$n.trees)
  score_data[, predictions := predictions]
  self$predictions <- score_data
  write.csv(score_data, "results.csv", row.names = FALSE)
}
