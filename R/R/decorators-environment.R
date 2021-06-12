#' Decorator that sets environment variables during step execution
#'
#' @param ... Named environment variables and their values. For example, 
#'   `environment_variables(foo = "bar")` will set the "foo" environment 
#'   variable as "bar" during step execution.
#'
#' @inherit decorator return
#' 
#' @export
#'
#' @examples \dontrun{
#' start <- function(self) {
#'   print(paste("The cutest animal is the", Sys.getenv("CUTEST_ANIMAL")))
#'   print(paste("The", Sys.getenv("ALSO_CUTE"), "is also cute, though"))
#' }
#' 
#' metaflow("EnvironmentVariables") %>%
#'   step(step="start", 
#'        environment_variables(CUTEST_ANIMAL = "corgi", ALSO_CUTE = "penguin"),
#'        r_function=start, 
#'        next_step="end") %>%
#'   step(step="end") %>% 
#'   run()
#' }
environment_variables <- function(...) {
  env_vars <- list(...)
  if (length(env_vars) == 0) {
    env_var_dict <- "{}"
  } else {
    env_vars_names <- names(env_vars)
    if (is.null(env_vars_names) || "" %in% env_vars_names) {
      stop("All environment variables must be named")
    }
    
    env_var_dict <- lapply(
      seq_along(env_vars),
      function(x) {
        paste0(
          escape_quote(env_vars_names[[x]]),
          ": ",
          escape_quote(env_vars[[x]])
        )
      }
    )
    env_var_dict <- paste0("{", paste(env_var_dict, collapse = ", "), "}")
  }
  
  decorator("environment", vars = env_var_dict, .convert_args = FALSE)
}