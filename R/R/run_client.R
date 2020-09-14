#' run_client
#' @description A R6 class representing a past run for an existing flow.
#' Instances of this class contain all steps related to a run.
#'
#' @docType class
#' @include package.R
#' @include metaflow_client.R
#' @include utils.R
#'
#' @return Object of \code{\link{R6Class}} with fields/methods for introspection.
#' @format \code{\link{R6Class}} object.
#'

#' @section Usage:
#' \preformatted{
#' r <- run_client$new(flow, run_id)
#' r <- run_client$new("HelloFlow/12")
#'
#' r$id
#' r$tags
#' r$finished_at
#' r$steps
#' r$artifacts
#' r$step("end")
#' r$artifact("script_name")
#' r$summary()
#' }
#'
#' @export
run_client <- R6::R6Class("RunClient",
  inherit = metaflow_object,
  public = list(
    #' @description Initialize the object from a \code{FlowClient} object and \code{run_id}
    #' @return \code{RunClient} R6 object
    #' @param ... The argument list can be either (1) a single \code{pathspec} string such as "HelloFlow/123"
    #' or (2) \code{(flow, run_id)}, where
    #' a \code{flow} is a parent \code{FlowClient} object which contains the run, and \code{run_id} is the identifier of the run.
    initialize = function(...) {
      arguments <- list(...)
      if (nargs() == 2) {
        flow <- arguments[[1]]
        run_id <- arguments[[2]]
        if (!is.character(run_id)) {
          run_id <- as.character(run_id)
        }
        if (run_id == "latest_run") {
          run_id <- flow$latest_run
        } else if (run_id == "latest_successful_run") {
          run_id <- flow$latest_successful_run
        } else {
          if (!run_id %in% flow$get_values()) {
            stop(
              "Not a valid run id",
              call. = FALSE
            )
          }
        }
        idx <- which(flow$get_values() == run_id)
        run <- reticulate::import_builtins()$list(flow$get_obj())[[idx]]
        super$initialize(run)
      } else if (nargs() == 1) {
        pathspec <- arguments[[1]]
        run <- pkg.env$mf$Run(pathspec)
        super$initialize(run)
      } else {
        stop("Wrong number of arguments. Please see help document for run_client")
      }
    },

    #' @description Create a \code{StepClient} object under this \code{run}
    #' @return StepClient R6 object
    #' @param step_id identifier of the step, for example "start" or "end"
    step = function(step_id) {
      step_client$new(self, step_id)
    },

    #' @description Fetch the data artifacts for the end step of this \code{run}.
    #' @return metaflow artifact
    #' @param name names of artifacts
    artifact = function(name) {
      blob <- super$get_obj()$data[[name]]
      return(mf_deserialize(blob))
    },

    #' @description Summary of the \code{run}
    summary = function() {
      successful <- self$finished
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
        cli::rule(left = paste0("Run Summary: ", self$id)), "\n",
        paste0(strrep(" ", 4), "Successful: ", strrep(" ", 11), successful, "\n"),
        paste0(strrep(" ", 4), "Created at: ", strrep(" ", 11), created_at, "\n"),
        paste0(strrep(" ", 4), "Finished at: ", strrep(" ", 10), finished_at, "\n"),
        paste0(strrep(" ", 4), "Time: ", strrep(" ", 17), time, "\n")
      )
    }
  ),

  active = list(
    #' @field super_ Get the metaflow object base class
    super_ = function() super,

    #' @field id The identifier of this run object.
    id = function() super$get_obj()$id,

    #' @field created_at The time of creation of this run object.
    created_at = function() super$get_obj()$created_at,

    #' @field pathspec The path spec that uniquely identifies this run object.
    # It looks like HelloWorldFlow/2 where 2 is the run_id
    pathspec = function() super$get_obj()$pathspec,

    #' @field parent The parent object (flow object) identifier of the current run object.
    parent = function() super$get_obj()$parent,

    #' @field tags A vector of strings representing tags assigned to this run object.
    tags = function() reticulate::import_builtins()$list(super$get_obj()$tags),

    ##' @field code Get the code package of the run if it exists
    code = function() super$get_obj()$code,

    #' @field end_task The task identifier, if available, corresponding to the end step of this run.
    end_task = function() super$get_obj()$end_task$id,

    #' @field finished The boolean flag identifying if the run has finished.
    finished = function() super$get_obj()$finished,

    #' @field finished_at The finish time, if available, of this run.
    finished_at = function() super$get_obj()$finished_at,

    #' @field successful The boolean flag identifying if the end task was successful.
    successful = function() super$get_obj()$successful,

    #' @field steps The vector of all step identifiers of this run.
    steps = function() super$get_values(),

    #' @field artifacts The vector of all data artifact identifiers produced by the end step of this run.
    artifacts = function() {
      tryCatch(names(py_get_attr(super$get_obj()$data, "_artifacts", silent = TRUE)),
        error = function(cond) {
          return(NULL)
        }
      )
    }
  ),
  lock_class = TRUE
)

#' Instantiates a new run object.
#'
#' @param flow_id Flow identifier.
#' @param run_id Run identifier.
#' @return \code{run} object corresponding to the supplied identifiers.
#' @export
new_run <- function(flow_id, run_id) {
  client <- mf_client$new()
  client$flow(flow_id)$run(run_id)
}
