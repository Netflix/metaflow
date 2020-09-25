Flow <- R6::R6Class("Flow",
  private = list(
    .name = NULL,
    .flow_decorators = NULL,
    .steps = NULL,
    .parameters = NULL,
    .functions = NULL
  ),
  public = list(
    initialize = function(name, flow_decorators) {
      stopifnot(is.character(name), length(name) == 1)
      private$.name <- name
      private$.flow_decorators <- flow_decorators
    },
    format = function() {
      print_flow(
        flow = private$.name,
        flow_decorators = private$.flow_decorators,
        parameters = private$.parameters,
        steps = private$.steps
      )
    },
    add_parameter = function(x) {
      if (!is.null(private$.parameters)) {
        private$.parameters <- c(private$.parameters, x)
      } else {
        private$.parameters <- x
      }
      invisible(self)
    },
    add_step = function(x) {
      private$.steps <- c(private$.steps, x)
      invisible(self)
    },
    add_function = function(x) {
      if (!is.null(private$.functions)) {
        private$.functions <- c(private$.functions, x)
      } else {
        private$.functions <- x
      }
      invisible(self)
    },
    get_flow = function(save = FALSE) {
      x <- print_flow(
        private$.name,
        private$.flow_decorators,
        private$.parameters,
        paste0(private$.steps, collapse = "")
      )
      if (save) {
        writeLines(x, con = "flow.py")
      } else {
        return(x)
      }
    },
    get_name = function() {
      private$.name
    },
    get_parameters = function() {
      private$.parameters
    },
    get_steps = function() {
      private$.steps
    },
    get_functions = function() {
      if (length(private$.functions) == 1) {
        private$.functions
      } else {
        private$.functions[!unlist(lapply(private$.functions, is.null))]
      }
    }
  )
)

header <- function(flow, flow_decorators = NULL) {
  imports <- paste0(c("FlowSpec", "step", "Parameter", "retry", "environment", "batch", "catch", "resources", "schedule"), collapse = ", ")
  paste0(
    "from metaflow import ", imports, space(1, type = "v"),
    "from metaflow.R import call_r", space(3, type = "v"),
    paste0(add_decorators(flow_decorators), collapse = ""),
    "class ", flow, "(FlowSpec):", space(1, type = "v")
  )
}

footer <- function(flow) {
  paste0(
    "FLOW=", flow, space(1, type = "v"),
    "if __name__ == '__main__':", space(1, type = "v"),
    space(4), flow, "()"
  )
}

print_flow <- function(flow, flow_decorators = NULL, parameters = NULL, steps = NULL) {
  paste0(c(
    header(flow, flow_decorators),
    parameters,
    steps,
    footer(flow)
  ),
  collapse = "\n"
  )
}
