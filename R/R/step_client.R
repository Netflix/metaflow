#' step_client
#' @description An R6 Class representing a step for a past run.
#' Instances of this class contain all tasks related to a step.
#'
#' @docType class
#' @include package.R
#' @include metaflow_client.R
#'
#' @return Object of \code{\link{R6Class}} with fields/methods for introspection.
#' @format \code{\link{R6Class}} object.
#'
#' @section Usage:
#' \preformatted{
#' s <- step_client$new(run, step_id)
#' s <- step_client$new("HelloWorldFlow/123/start")
#'
#' s$id
#' s$tags
#' s$finished_at
#' s$tasks
#' s$task("12")
#' s$summary()
#' }
#'
#' @export
step_client <- R6::R6Class("StepClient",
  inherit = metaflow_object,
  public = list(
    #' @description Initialize a \code{StepClient} object
    #' @return a \code{StepClient} object
    #' @param ... The argument list can be either (1) a single \code{pathspec} string such as "MyFlow/123/start" or (2) \code{(run, step_id)}, where
    #' \code{run} is a parent \code{RunClient} object which contains the step, and \code{step_id} is the name/id of the step such as "start".
    initialize = function(...) {
      arguments <- list(...)
      if (nargs() == 2) {
        run <- arguments[[1]]
        step_id <- arguments[[2]]
        if (!step_id %in% run$get_values()) {
          stop(
            "Not a valid step id",
            call. = FALSE
          )
        }
        idx <- which(run$get_values() == step_id)
        step <- reticulate::import_builtins()$list(run$get_obj())[[idx]]
        super$initialize(step)
      } else if (nargs() == 1) {
        pathspec <- arguments[[1]]
        step <- pkg.env$mf$Step(pathspec)
        super$initialize(step)
      } else {
        stop("Wrong number of arguments. Please see help document for step_client.")
      }
    },

    #' @description create a \code{TaskClient} object of the current step
    #' @return a \code{TaskClient} object
    #' @param task_id the identifier of the task
    task = function(task_id) {
      task_client$new(self, task_id)
    },

    #' @description summary of the current step
    summary = function() {
      tasks <- length(self$tasks)
      created_at <- substring(self$created_at, 1, 20)
      finished_at <- substring(self$finished_at, 1, 20)
      difftime <- lubridate::ymd_hms(finished_at) - lubridate::ymd_hms(created_at)
      unit <- attr(difftime, "units")
      if (length(finished_at) == 0) {
        time <- ""
      } else {
        time <- paste0(round(as.numeric(difftime), 2), " ", unit)
      }
      cat(
        cli::rule(left = paste0("Step Summary: ", self$id)), "\n",
        paste0(strrep(" ", 4), "# Tasks: ", strrep(" ", 14), tasks, "\n"),
        paste0(strrep(" ", 4), "Created at: ", strrep(" ", 11), created_at, "\n"),
        paste0(strrep(" ", 4), "Finished at: ", strrep(" ", 10), finished_at, "\n"),
        paste0(strrep(" ", 4), "Time: ", strrep(" ", 17), time, "\n")
      )
    }
  ),

  active = list(
    #' @field super_ Access the R6 metaflow object base class
    super_ = function() super,

    #' @field id The identifier of this step object.
    id = function() super$get_obj()$id,

    #' @field created_at The time of creation of this step object.
    created_at = function() super$get_obj()$created_at,

    #' @field pathspec The path spec that uniquely identifies this step object,
    #  for example, HellowWorldFlow/2/start.
    pathspec = function() super$get_obj()$pathspec,

    #' @field parent The parent object (run object) identifier of this step object.
    parent = function() super$get_obj()$parent,

    #' @field tags A vector of strings representing tags assigned to this step object.
    tags = function() reticulate::import_builtins()$list(super$get_obj()$tags),

    #' @field finished_at The finish time, if available, of this step.
    finished_at = function() super$get_obj()$finished_at,

    #' @field a_task Any task id of the current step
    a_task = function() super$get_obj()$task$id,

    #' @field tasks All task ids of the current step
    tasks = function() super$get_values()
  ),
  lock_class = TRUE
)

#' Instantiates a new step object.
#'
#' @param flow_id Flow identifier.
#' @param run_id Run identifier.
#' @param step_id Step identifier.
#' @return \code{step} object corresponding to the supplied identifiers.
#' @export
new_step <- function(flow_id, run_id, step_id) {
  client <- mf_client$new()
  client$flow(flow_id)$run(run_id)$step(step_id)
}
