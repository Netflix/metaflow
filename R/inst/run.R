suppressPackageStartupMessages(library(metaflow))

flowRDS_file <- "flow.RDS"
flowRDS_arg <- Filter(function(arg) {
  startsWith(arg, "--flowRDS")
}, commandArgs())
if (length(flowRDS_arg) == 1) {
  flowRDS_file <- strsplit(flowRDS_arg[1], "=")[[1]][2]
} else {
  stop("missing --flowRDS file command in the command line arguments")
}

if (!file.exists(flowRDS_file)) {
  stop(sprintf("Cannot locate flow RDS file: %s", flowRDS_file))
}

flow <- readRDS(flowRDS_file)

rfuncs <- flow$get_functions()
r_functions <- reticulate::dict(rfuncs, convert = TRUE)
flow_script <- flow$get_flow()

for (fname in names(rfuncs)) {
  assign(fname, rfuncs[[fname]], envir = .GlobalEnv)
}

runtime_args <- function(arg) {
  return(!startsWith(arg, "--flowRDS"))
}

mf <- reticulate::import("metaflow", delay_load = TRUE)

mf$R$run(
  flow_script, r_functions,
  flowRDS_file,
  Filter(runtime_args, commandArgs(trailingOnly = TRUE)),
  c(commandArgs(trailingOnly = FALSE), flowRDS_arg),
  metaflow_location(flowRDS = flowRDS_file),
  container_image(),
  r_version(),
  paste(R.version.string),
  paste(getRversion())
)
