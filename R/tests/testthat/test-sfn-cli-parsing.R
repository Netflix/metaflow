test_that("SFN create", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R step-functions create"
  system(cmd)

  # Rscript /Library/../metaflow/run.R --flowRDS=flow.RDS step-functions create
  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint  step-functions create" 

  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})

test_that("SFN create --help", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R step-functions create --help"
  system(cmd)

  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint step-functions create --help" 

  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})

test_that("SFN create --package-suffixes", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R --package-suffixes=.csv,.RDS,.R step-functions create"
  system(cmd)

  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint --package-suffixes=.csv,.RDS,.R step-functions create" 

  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})

test_that("SFN create --generate-new-token", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R step-functions create --generate-new-token"
  system(cmd)

  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint  step-functions create --generate-new-token" 

  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})

test_that("SFN create --generate-new-token --max-workers 100 --lr 0.01", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R step-functions create --generate-new-token --max-workers 100 --lr 0.01"
  system(cmd)

  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint  step-functions create --generate-new-token --max-workers 100 --lr 0.01" 
  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})


test_that("SFN trigger", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R step-functions trigger"
  system(cmd)

  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint  step-functions trigger" 

  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})


test_that("SFN list-runs --running", {
  skip_if_no_metaflow()

  cmd <- "Rscript test-run-cmd.R step-functions list-runs --running"
  system(cmd)

  run_cmd <- strsplit(trimws(readRDS("run_cmd.RDS")), split=" ")[[1]]
  actual <- paste(run_cmd[3:length(run_cmd)], collapse=" ")

  expected <- "--flowRDS=flow.RDS --no-pylint  step-functions list-runs --running" 

  expect_equal(actual, expected)
  on.exit(file.remove("run_cmd.RDS"))
})
