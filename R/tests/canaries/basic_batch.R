# -*- coding: utf-8 -*-
library(metaflow)

start <- function(self) {
  print("start step")
  return(0)
}

a <- function(self) {
  print("hello world!")
  return(0)
}

end <- function(self) {
  print("end step")
}

test_flow <- metaflow("BasicHelloWorldFlow") %>%
  step(
    step = "start",
    r_function = start,
    foreach = NULL,
    next_step = "a",
    join = FALSE
  ) %>%
  step(
    step = "a",
    r_function = a,
    foreach = NULL,
    next_step = "end",
    join = FALSE
  ) %>%
  step(
    step = "end",
    r_function = end,
    foreach = NULL,
    join = FALSE
  )

status_code <- test_flow %>% run(
  batch = TRUE, with = c("retry", "catch"), max_workers = 16,
  package_suffixes = c(".R", ".py", ".csv"), metadata = "service", datastore = "s3"
)
