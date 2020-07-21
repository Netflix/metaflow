decorated_function <- function(f, type = NULL, prio = NULL, qual = c(), required = FALSE) {
  attr(f, "type") <- type
  attr(f, "prio") <- prio
  attr(f, "quals") <- qual
  attr(f, "required") <- required
  return(f)
}

assert_exception <- function(r_expr, expected_error_message, env = parent.frame()) {
  has_correct_error_message <- FALSE
  tryCatch(
    {
      eval(r_expr, envir = env)
    },
    error = function(e) {
      print(e)
      has_correct_error_message <<-
        (length(grep(expected_error_message, e$message)) > 0)
    }
  )
  stopifnot(has_correct_error_message)
}
