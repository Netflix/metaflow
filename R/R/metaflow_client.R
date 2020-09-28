#' Instantiate Metaflow flow/run/step/task client
#' @description A R6 Class representing a MetaflowClient used to inspect flow/run/step/task artifacts.
#' This is a factory class that provides convenience for creating Flow/Run/Step/Task Client objects.
#'
#' @docType class
#'
#' @return Object of \code{\link{R6Class}} with fields/methods for introspection.
#' @format \code{\link{R6Class}} object.
#'

#'
#' @section Usage:
#' \preformatted{
#' client <- mf_flow$new()
#'
#' f <- client$flow("HelloWorldFlow")
#'
#' r <- client$run(f, run_id)
#' r <- client$flow('HelloWorldFlow')$run(run_id)
#'
#' s <- client$step(r, step_id)
#' s <- client$flow('HelloWorldFlow')$run(run_id)$step(step_id)
#'
#' t <- client$task(s, task_id)
#' t <- client$flow('HelloWorldFlow')$run(run_id)$step(step_id)$task(task_id)
#'
#' }
#' @export
mf_client <- R6::R6Class(
  "MetaflowClient",
  public = list(
    #' @description
    #' Create a metaflow FlowClient R6 object based on flow_id.
    #' @return R6 object representing the FlowClient object
    #' @param flow_id the name/id of the flow for inspection, for example "HelloWorldFlow"
    flow = function(flow_id) {
      flow_client$new(flow_id)
    },

    #' @description
    #' Create a metaflow RunClient R6 object from a FlowClient R6 object and run_id
    #' @return R6 object representing the RunClient object
    #' @param flow_client R6 object
    #' @param run_id run id
    run = function(flow_client, run_id) {
      run_client$new(flow_client, run_id)
    },

    #' @description
    #' Create a metaflow StepClient R6 object from RunClient R6 object and step_id
    #' @return R6 object representing the StepClient object
    #' @param run_client run_client
    #' @param step_id step id
    step = function(run_client, step_id) {
      step_client$new(run_client, step_id)
    },

    #' @description
    #' Create a metaflow StepClient R6 object from RunClient R6 object and step_id
    #' @return R6 object representing the StepClient object
    #' @param step_client step client
    #' @param task_id task id
    task = function(step_client, task_id) {
      task_client$new(step_client, task_id)
    }
  )
)



#' Metaflow object base class
#'
#' @description A Reference Class to represent a metaflow object.
#'
#' @docType class
#'
#' @return Object of \code{\link{R6Class}} with fields/methods for introspection.
#' @format \code{\link{R6Class}} object.
#'
metaflow_object <- R6::R6Class(
  "metaflow_object",
  public = list(
    #' @description Initialize a metaflow object
    #' @param obj the python metaflow object
    initialize = function(obj = NA) {
      if (!inherits(obj, "metaflow.client.core.MetaflowObject")) {
        stop("Must be a metaflow object", call. = FALSE)
      }
      private$obj_ <- obj
      private$id_ <- obj$id
      private$created_at_ <- obj$created_at
      private$parent_ <- obj$parent$id
      private$pathspec_ <- obj$pathspec
      private$tags_ <- reticulate::import_builtins()$list(obj$tags)

      # TODO: handle after Core Convergence
      # The OSS version of MetaflowObject class does not have url_path property
      # which returns the URL of this object at the Metaflow service.
      # self$url_path <- private$obj$url_path
    },

    #' @description Check if this metaflow object is in current namespace
    #' @return TRUE/FALSE
    is_in_namespace = function() {
      private$obj_$is_in_namespace()
    },

    #' @description Get the python metaflow object
    #' @return python (reticulate) metaflow object
    get_obj = function() private$obj_,

    #' @description Get values of current metaflow object
    #' @return a list of lower level metaflow objects
    get_values = function() extract_ids(private$obj_)
  ),
  private = list(
    obj_ = NULL,
    id_ = NULL,
    created_at_ = NULL,
    parent_ = NULL,
    pathspec_ = NULL,
    tags_ = NULL
  ),
  active = list(
    #' @field id The identifier of this object.
    id = function() private$id_,

    #' @field created_at The time of creation of this object.
    created_at = function() private$created_at_,

    #' @field parent The parent object identifier of this current object.
    parent = function() private$parent_,

    #' @field pathspec The path spec that uniquely identifies this object.
    pathspec = function() private$pathspec_,

    #' @field tags The vector of tags assigned to this object.
    tags = function() private$tags_
  )
)

`[.metaflow_object` <- function(x, i, ...) {
  x <- x$get_values()
  NextMethod()
}
