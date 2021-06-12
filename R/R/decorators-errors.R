#' Decorator that configures a step to retry upon failure
#' 
#' @description 
#' Use this decorator to configure a step to retry if it fails. Alternatively,
#' retry _any_ failing steps in an entire flow with `run(with = c("retry")`.
#' 
#' See \url{https://docs.metaflow.org/v/r/metaflow/failures} for more 
#' information on how to use this decorator.
#'
#' @param times Integer number of times to retry this step. Defaults to `3`. Set
#'   this to `0` to forbid a step from retrying at all. This may be useful
#'   when a step is not idempotent, and could have undesirable side-effects if
#'   retried.
#' @param minutes_between_retries Integer Number of minutes between retries.
#'   Defaults to `2`.
#'   
#' @inherit decorator return
#'   
#' @export
#'
#' @examples \dontrun{
#' # Set up a step that fails 50% of the time, and retries it up to 3 times
#' # until it succeeds
#' start <- function(self){
#'   n <- rbinom(n=1, size=1, prob=0.5)
#'   if (n==0){
#'     stop("Bad Luck!") 
#'   } else{
#'     print("Lucky you!")
#'   }
#' }
#' 
#' end <- function(self){
#'   print("Phew!")
#' }
#' 
#' metaflow("RetryFlow") %>%
#'   step(step="start", 
#'        retry(times=3),
#'        r_function=start, 
#'        next_step="end") %>%
#'   step(step="end", 
#'        r_function=end) %>% 
#'   run()
#' }
retry <- function(times = 3L, minutes_between_retries = 2L) {
  decorator(
    "retry",
    times = times,
    minutes_between_retries = minutes_between_retries
  )
}

#' Decorator that configures a step to catch an error
#'
#' @description 
#' Use this decorator to configure a step to catch any errors that occur during
#' evaluation. For steps that can't be safely retried, it is a good idea to use
#' this decorator along with `retry(times = 0)`.
#' 
#' See \url{https://docs.metaflow.org/v/r/metaflow/failures#catching-exceptions-with-the-catch-decorator}
#' for more information on how to use this decorator.
#'
#' @param var Character. Name of the artifact in which to store the caught
#' exception. If `NULL` (the default), the exception is not stored.
#' @param print_exception Boolean. Determines whether or not the exception is
#'   printed to stdout when caught. Defaults to `TRUE`.
#'   
#' @inherit decorator return
#'
#' @export
#'
#' @examples \donttest{
#' 
#' start <- function(self) {
#'   stop("Oh no!")
#' }
#' 
#' end <- function(self) {
#'   message(
#'     "Error is : ", self$start_failed
#'   )
#' }
#' 
#' metaflow("AlwaysErrors") %>%
#'   step(
#'     catch(var = "start_failed"),
#'     retry(times = 0),
#'     step = "start",
#'     r_function = start,
#'     next_step = "end"
#'   ) %>%
#'   step(
#'     step = "end",
#'     r_function = end
#'   ) %>%
#'   run()
#' }
catch <- function(var = NULL, print_exception = TRUE) {
  decorator("catch", var = var, print_exception = print_exception)
}

