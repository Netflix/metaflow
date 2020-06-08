library(metaflow)
source("./house-price-prediction-functions.R")


metaflow("HousingFlow") %>%
  step(
    step = "start",
    next_step = "load_training_data"
  ) %>%
  step(
    step = "load_training_data",
    r_function = load_training_data,
    next_step = "clean_training_data"
  ) %>%
  step(
    step = "clean_training_data",
    r_function = clean_data_set,
    next_step = "parameter_grid"
  ) %>%
  step(
    step = "parameter_grid",
    r_function = parameter_grid,
    next_step = "fit_models",
    foreach = "parameters"
  ) %>%
  step(
    step = "fit_models",
    r_function = fit_models,
    next_step = "join"
  ) %>%
  step(
    step = "join",
    r_function = join,
    next_step = "select_best_fit",
    join = TRUE
  ) %>%
  step(
    step = "select_best_fit",
    r_function = select_best_fit,
    next_step = "score_data"
  ) %>%
  step(
    step = "score_data",
    r_function = score_data,
    next_step = "end"
  ) %>%
  step(step = "end") %>%
  run(package_suffixes = c(".R", ".py", ".csv"), 
   batch = TRUE,  max_workers = 16,
  metadata='service', datastore='s3')
