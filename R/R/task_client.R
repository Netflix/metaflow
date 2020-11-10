#' task_client
#' @description An R6 Class representing a task for a step.
#' Instances of this class contain all data artifacts related to a task.
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
#' t <- task_client$new(step, task_id)
#' t <- task_client$new("HelloFlow/12/start/139423")
#'
#' t$id
#' t$tags
#' t$finished_at
#' t$artifacts
#' t$artifact(t$artifacts)
#' t$summary()
#' }
#'
#' @export
task_client <- R6::R6Class("TaskClient",
  inherit = metaflow_object,
  public = list(
    #' @description Initialize a \code{TaskClient} object
    #' @return a \code{TaskClient} object
    #' @param ... The argument list can be either (1) a single \code{pathspec} string such as "HelloFlow/123/start/293812"
    #' or (2) \code{(step, task_id)}, where
    #' a \code{step} is a parent \code{StepClient} object which contains the run, and \code{task_id} is the identifier of the task.
    initialize = function(...) {
      arguments <- list(...)
      if (nargs() == 2) {
        step <- arguments[[1]]
        task_id <- arguments[[2]]
        idx <- which(step$get_values() == task_id)
        task <- reticulate::import_builtins()$list(step$get_obj())[[idx]]
        super$initialize(task)
      } else if (nargs() == 1) {
        pathspec <- arguments[[1]]
        task <- pkg.env$mf$Task(pathspec)
        super$initialize(task)
      } else {
        stop("Wrong number of arguments. Please see help document for task_client")
      }
    },

    #' @description Fetch the data artifacts for this task
    #' @return metaflow artifact
    #' @param name names of artifacts
    artifact = function(name) {
      blob <- super$get_obj()$data[[name]]
      return(mf_deserialize(blob))
    },

    #' @description Summary of the task
    summary = function() {
      successful <- self$successful
      created_at <- self$created_at
      finished_at <- substring(self$finished_at, 1, 20)
      difftime <- lubridate::ymd_hms(finished_at) - lubridate::ymd_hms(created_at)
      unit <- attr(difftime, "units")
      if (length(finished_at) == 0) {
        time <- ""
      } else {
        time <- paste0(round(as.numeric(difftime), 2), " ", unit)
      }
      objects <- paste(x$artifacts, collapse = paste(c("\n", strrep(" ", 28)), collapse = ""))
      cat(
        cli::rule(left = paste0("Task Summary: ", self$id)), "\n",
        paste0(strrep(" ", 4), "Successful: ", strrep(" ", 11), successful, "\n"),
        paste0(strrep(" ", 4), "Created At: ", strrep(" ", 11), created_at, "\n"),
        paste0(strrep(" ", 4), "Finished At: ", strrep(" ", 10), finished_at, "\n"),
        paste0(strrep(" ", 4), "Time: ", strrep(" ", 17), time, "\n"),
        paste0(strrep(" ", 4), "Objects: ", strrep(" ", 14), objects, "\n")
      )
    }
  ),
  active = list(
    #' @field super_ Get the metaflow object base class
    super_ = function() super,

    #' @field id The identifier of this task object.
    id = function() super$get_obj()$id,

    #' @field pathspec The path spec that uniquely identifies this task object,
    # for example, HelloWorldFlow/2/start/231
    pathspec = function() super$get_obj()$pathspec,

    #' @field parent The parent object (step object) identifier of this task object.
    parent = function() super$get_obj()$parent,

    #' @field tags A vector of strings representing tags assigned to this task object.
    tags = function() reticulate::import_builtins()$list(super$get_obj()$tags),

    #' @field exception The exception that caused this task to fail.
    exception = function() super$get_obj()$exception,

    #' @field created_at The time of creation of this task.
    created_at = function() super$get_obj()$created_at,

    #' @field finished The boolean flag identifying if the task has finished.
    finished = function() super$get_obj()$finished,

    #' @field finished_at The finish time, if available, of this task.
    finished_at = function() super$get_obj()$finished_at,

    #' @field code Get the code package of the run if it exists
    code = function() super$get_obj()$code,

    #' @field index The index of the innermost foreach loop,
    # if the task is run inside one or more foreach loops.
    index = function() {
      tryCatch(super$get_obj()$index,
        error = function(cond) {
          return(NULL)
        }
      )
    },

    #' @field metadata_dict The dictionary of
    # metadata events produced by this task.
    metadata_dict = function() super$get_obj()$metadata_dict,

    #' @field runtime_name The name of the runtime environment
    # where this task was run.
    runtime_name = function() super$get_obj()$runtime_name,

    #' @field stderr The full stderr output of this task.
    stderr = function() super$get_obj()$stderr,

    #' @field stdout The full stdout output of this task.
    stdout = function() super$get_obj()$stdout,

    #' @field successful The boolean flag identifying if
    # the task has finished successfully.
    successful = function() super$get_obj()$successful,

    #' @field artifacts The vector of artifact ids produced by this task.
    artifacts = function() super$get_values()
  ),
  lock_class = TRUE
)

#' Instantiates a new task object.
#'
#' @param flow_id Flow identifier.
#' @param run_id Run identifier.
#' @param step_id Step identifier.
#' @param task_id Task identifier.
#' @return \code{task} object corresponding to the supplied identifiers.
#' @export
new_task <- function(flow_id, run_id, step_id, task_id) {
  client <- mf_client$new()
  client$flow(flow_id)$run(run_id)$step(step_id)$task(task_id)
}
