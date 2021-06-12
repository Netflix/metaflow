test_that("@environment parses correctly", {
  skip_if_no_metaflow()
  
  actual <- decorator("retry", times = 3)[1]
  expected <- "@retry(times=3)"
  expect_equal(actual, expected)
})

test_that("@environment wrapper parses correctly", {
  skip_if_no_metaflow()
  
  actual <- environment_variables(foo = "red panda")[1]
  expected <- "@environment(vars={'foo': 'red panda'})"
  expect_equal(actual, expected)
  
  actual <- environment_variables(foo = "red panda", bar = "corgi")[1]
  expected <- "@environment(vars={'foo': 'red panda', 'bar': 'corgi'})"
  expect_equal(actual, expected)
})