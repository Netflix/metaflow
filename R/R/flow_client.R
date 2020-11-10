#' flow_client
#' @description An R6 Class representing an existing flow with a certain id.
#' Instances of this class contain all runs related to a flow.
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
#' f <- flow_client$new(flow_id)
#'
#' f$id
#' f$tags
#' f$latest_run
#' f$latest_successful_run
#' f$runs
#' f$run(f$latest_run)
#' f$summary()
#' }
#'

#' @export
flow_client <- R6::R6Class("FlowClient",
  inherit = metaflow_object,
  public = list(
    #' @description Initialize the object from flow_id
    #' @return FlowClient R6 object
    #' @param flow_id, name/id of the flow such as "HelloWorldFlow"
    initialize = function(flow_id) {
      flow <- pkg.env$mf$Flow(flow_id)
      super$initialize(flow)
    },

    #' @description Get a RunClient R6 object of any run in this flow based on run_id
    #' @return RunClient R6 object
    #' @param run_id, id of the specific run within this flow
    run = function(run_id) {
      run_client$new(self, run_id)
    },

    #' @description Get a list of run_ids which has the specific tag
    #' @return A list of run_client R6 object
    #' @param ... the specific tags (string) we need to have for the runs
    runs_with_tags = function(...) {
      run_objs <- reticulate::import_builtins()$list(super$get_obj()$runs(...))
      return(invisible(lapply(run_objs, function(run) {
        run_client$new(self, run$id)
      })))
    },

    #' @description Summary of this flow
    summary = function() {
      created_at <- self$created_at
      latest_run <- self$latest_run
      last_successful_run <- self$latest_successful_run
      number_runs <- length(self$runs)
      cat(
        cli::rule(left = paste0("Flow Summary: ", self$id)), "\n",
        paste0(strrep(" ", 4), "Created At: ", strrep(" ", 13), created_at, "\n"),
        paste0(strrep(" ", 4), "Latest Run: ", strrep(" ", 13), latest_run, "\n"),
        paste0(strrep(" ", 4), "Latest Successful Run: ", strrep(" ", 2), last_successful_run, "\n"),
        paste0(strrep(" ", 4), "Runs: ", strrep(" ", 19), number_runs, "\n")
      )
    }
  ),
  active = list(
    #' @field super_ Access the R6 metaflow object base class
    super_ = function() super,

    #' @field pathspec The path spec that uniquely identifies this flow object
    #  Since flow is a top level object, its pathspec is simply the flow name.
    pathspec = function() super$get_obj()$pathspec,

    #' @field parent The parent object identifier of this current flow object.
    # Since flow is a top level object, its parent is always NULL.
    parent = function() super$get_obj()$parent,

    #' @field tags The vector of tags assigned to this object.
    tags = function() reticulate::import_builtins()$list(super$get_obj()$tags),

    #' @field created_at The time of creation of this flow object.
    created_at = function() super$get_obj()$created_at,

    #' @field finished_at The finish time, if available, of this flow.
    finished_at = function() super$get_obj()$finished_at,

    #' @field latest_run The latest run identifier of this flow.
    latest_run = function() super$get_obj()$latest_run$id,

    #' @field latest_successful_run  The latest successful run identifier of this flow.
    latest_successful_run = function() super$get_obj()$latest_successful_run$id,

    #' @field runs The vector of all run identifiers of this flow.
    runs = function() super$get_values()
  ),
  lock_class = TRUE
)

#' Instantiates a new flow object.
#'
#' @param flow_id Flow identifier.
#' @return \code{flow} object corresponding to the supplied identifier.
#' @export
new_flow <- function(flow_id) {
  flow_client$new(flow_id)
}
