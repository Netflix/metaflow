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

test_that("@catch parses correctly", {
  skip_if_no_metaflow()
  
  actual <- decorator("catch", var = "red_panda")[1]
  expected <- "@catch(var='red_panda')"
  expect_equal(actual, expected)
})

test_that("@catch wrapper parses correctly", {
  skip_if_no_metaflow()
  
  actual <- catch(var = "red_panda")[1]
  expected <- "@catch(var='red_panda', print_exception=True)"
  expect_equal(actual, expected)
  
  actual <- catch(var = "red_panda", print_exception = FALSE)[1]
  expected <- "@catch(var='red_panda', print_exception=False)"
  expect_equal(actual, expected)
})
