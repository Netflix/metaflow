test_that("@resources parses correctly", {
  skip_if_no_metaflow()
  actual <- decorator("resources", cpu = 16, memory = 220000, disk = 150000, network = 4000)[1]
  expected <- "@resources(cpu=16, memory=220000, disk=150000, network=4000)"
  expect_equal(actual, expected)
})

test_that("@batch parses correctly", {
  skip_if_no_metaflow()
  actual <- decorator("batch", memory = 60000, cpu = 8)[1]
  expected <- "@batch(memory=60000, cpu=8)"
  expect_equal(actual, expected)
})

test_that("@resources wrapper parsed correctly", {
  skip_if_no_metaflow()
  
  actual <- resources()[1]
  expected <- paste0("@resources(",
                     "cpu=1, ",
                     "gpu=0, ",
                     "memory=4096, ",
                     "shared_memory=None",
                     ")")
  expect_equal(actual, expected)
  
  expect_match(resources(gpu = 1)[1], "gpu=1")
  expect_match(resources(memory = 60000)[1], "memory=60000")
})


test_that("@batch wrapper parsed correctly", {
  skip_if_no_metaflow()
  on.exit(metaflow_load()) # Restore the config

  pkg.env$mf$metaflow_config$BATCH_JOB_QUEUE <- "foo"
  pkg.env$mf$metaflow_config$ECS_S3_ACCESS_IAM_ROLE <- "bar"
  pkg.env$mf$metaflow_config$ECS_FARGATE_EXECUTION_ROLE <- "baz"
  
  actual <- batch()[1]
  expected <- paste0("@batch(",
                       "cpu=1, ",
                       "gpu=0, ",
                       "memory=4096, ",
                       "image=None, ",
                       "queue='foo', ",
                       "iam_role='bar', ",
                       "execution_role='baz', ",
                       "shared_memory=None, ",
                       "max_swap=None, ",
                       "swappiness=None",
                     ")")
  expect_equal(actual, expected)
  
  expect_match(batch(gpu = 1)[1], "gpu=1")
  expect_match(batch(iam_role = "cassowary")[1], "iam_role='cassowary'")
})
