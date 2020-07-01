#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
NULL

#' @importFrom reticulate dict
#' @export
reticulate::dict

#' @importFrom reticulate r_to_py
#' @export
reticulate::r_to_py

#' @importFrom reticulate py_to_r
#' @export
reticulate::py_to_r

simple_type <- function(obj){
  if (is.atomic(obj)){
    return(TRUE)
  }else if (is.list(obj)){
    for (item in obj){
      if (!simple_type(item)){
        return(FALSE)
      }
    } 
    return(TRUE) 
  } else {
    return(FALSE)
  }
}

#' Helper utility to serialize R object to metaflow
#' data format
#'
#' @param object object to serialize
#' @return metaflow data format object
mf_serialize <- function(object) {
  if (simple_type(object)){
    return(object)
  } else{
    return(serialize(object, NULL))
  }
}

#' Helper utility to deserialize objects from metaflow
#' data format to R object
#'
#' @param object object to deserialize
#' @return R object
mf_deserialize <- function(object) {
  r_obj <- object

  if (is.raw(object)){ 
    # for bytearray try to unserialize
    tryCatch({
      r_obj <- object %>% unserialize()
    }, error = function(e) {
      r_obj <- object
    })
  } 
  
  return(r_obj)
}

#' Overload getter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name 
#'
#' @section Usage:
#' \preformatted{
#'  print(self$var)
#' }
#' @export
'$.metaflow.flowspec.FlowSpec' <- function(self, name){
   value <- NextMethod(name)
   mf_deserialize(value)
}

#' Overload setter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name 
#' @param value value to assign to the attribute
#'
#' @section Usage:
#' \preformatted{
#'  self$var <- "hello"
#' }
#' @export
'$<-.metaflow.flowspec.FlowSpec' <- function(self, name, value){
  value <- mf_serialize(value) 
  NextMethod(name, value)
}

#' Overload getter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name 
#'
#' @section Usage:
#' \preformatted{
#'  print(self[["var"]])
#' }
#' @export
'[[.metaflow.flowspec.FlowSpec' <- function(self, name){
  value <- NextMethod(name)
  mf_deserialize(value)
}

#' Overload setter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name 
#' @param value value to assign to the attribute
#'
#' @section Usage:
#' \preformatted{
#'  self[["var"]] <- "hello"
#' }
#' @export
'[[<-.metaflow.flowspec.FlowSpec' <- function(self, name, value){
  value <- mf_serialize(value) 
  NextMethod(name, value)
}

#' Helper utility to gather inputs in a join step
#'
#' @param inputs inputs from parent branches
#' @param input field to extract from inputs from 
#' parent branches into vector
#' @section usage:
#' \preformatted{
#' gather_inputs(inputs, "alpha")
#' }
#' @export
gather_inputs <- function(inputs, input) {
  lapply(seq_along(inputs), function(x) {
    inputs[[x]][[input]]
  })
}

#' Helper utility to merge artifacts in a join step
#'
#' @param flow flow object
#' @param inputs inputs from parent branches
#' @param exclude list of artifact names to exclude from merging
#' @examples
#' \dontrun{
#' merge_artifacts(flow, inputs)
#' }
#' \dontrun{
#' merge_artifacts(flow, inputs, list("alpha"))
#' }
#' @export
merge_artifacts <- function(flow, inputs, exclude = list()) {
  flow$merge_artifacts(unname(inputs), exclude)
}

#' Helper utility to access current IDs of interest
#'
#' @param value one of flow_name, run_id, origin_run_id,
#'              step_name, task_id, pathspec, namespace,
#'              username
#' @examples
#' \dontrun{
#' current("flow_name")
#' }
#' @export
current <- function(value) {
  mf$current[[value]]
}

escape_bool <- function(x) {
  ifelse(x, "True", "False")
}

escape_quote <- function(x) {
  if (x %in% c("TRUE", "FALSE")) {
    ifelse(x == "TRUE", "True", "False")
  } else {
    encodeString(x, quote = "'")
  }
}

space <- function(len, type = "h") {
  switch(type,
         "h" = strrep(" ", len),
         "v" = strrep("\n", len)
  )
}

wrap_argument <- function(x) {
  x <- x[[1]]
  if (is.character(x)) {
    x <- escape_quote(x)
  }
  if (is.logical(x)) {
    x <- escape_bool(x)
  }
  x
}

python_3 <- function() {
  system("which python3", intern = TRUE)
}

#' Return installation path of metaflow R library 
#' @param flowRDS path of the RDS file containing the flow object
#' @export
metaflow_location <- function(flowRDS) {
  list(
    package = system.file(package = "metaflow"),
    flow = suppressWarnings(normalizePath(flowRDS)),
    wd = suppressWarnings(normalizePath(paste0(getwd())))
  )
}

extract_ids <- function(obj) {
  extract_str <- function(x) {
    chr <- as.character(x)
    gsub("'", "", regmatches(chr, gregexpr("'([^']*)'", chr))[[1]])
  }
  unlist(lapply(import_builtins()$list(obj), 
    function(x){sub(".*/", "", extract_str(x))}))
}

extract_str <- function(x) {
  chr <- as.character(x)
  gsub("'", "", regmatches(chr, gregexpr("'([^']*)'", chr))[[1]])
}

#' Return a vector of all flow ids.
#'
#' @export
list_flows <- function() {
  mf$Metaflow()$flows %>%
    extract_ids()
}

