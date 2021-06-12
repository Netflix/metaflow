test_that("@retry parses correctly", {
  skip_if_no_metaflow()
  
  actual <- decorator("retry", times = 3)[1]
  expected <- "@retry(times=3)"
  expect_equal(actual, expected)
})

test_that("@retry wrapper parses correctly", {
  skip_if_no_metaflow()
  
  actual <- retry(times = 3)[1]
  expected <- "@retry(times=3, minutes_between_retries=2)"
  expect_equal(actual, expected)
  
  actual <- retry(times = 3, minutes_between_retries=0)[1]
  expected <- "@retry(times=3, minutes_between_retries=0)"
  expect_equal(actual, expected)
})