#' Run metaflow
#'
#' @description
#' `run()` passes all command line arguments to metaflow.
#' These are captured whether running from interactive session or via `Rscript`
#'
#'
#' @param flow metaflow object
#' @param ... passed command line arguments
#' @details Command line arguments:
#' * package_suffixes: any file suffixes to include in the run
#'     * ex: c('.csv', '.R', '.py')
#' * datastore: 'local' (default) or 's3'
#' * metadata:  'local' (default) or 'service'
#' * batch: request flow to run on batch (default FALSE)
#' * resume: resume flow from last failed step
#'     * logical (default FALSE)
#' * with: any flow level decorators to include in the run
#'     * ex: c('retry', 'batch', 'catch')
#' * max_workers: limits the number of tasks run in parallel
#' * max_num_splits: maximum number of parallel splits allowed
#' * other_args: escape hatch to provide args not covered above
#' * key=value: any parameters specified as part of the flow
#' @section Usage:
#' \preformatted{
#' run(flow, batch = TRUE, with = c("retry", "catch"), max_workers = 16, max_num_splits = 200)
#' run(flow, alpha = 0.01)
#' }
#' @export
run <- function(flow = NULL, ...) {
  flow_file <- tempfile(flow$get_name(), tmpdir = ".", fileext = ".RDS")
  tryCatch(
    {
      saveRDS(flow, flow_file)
    },
    error = function(e) {
      stop(sprintf("Cannot create temporary RDS file %s", flow_file))
    }
  )

  cmd <- run_cmd(flow_file = flow_file, ...)
  #message(paste0("Flow cli:\n", cmd))
  status_code <- system(cmd)
  invisible(file.remove(flow_file))
  return(invisible(status_code))
}

run_cmd <- function(flow_file, ...) {
  run_options <- list(...)
  flags <- flags(...)

  run_path <- system.file("run.R", package = "metaflow")

  if ("resume" %in% names(flags)) {
    if (is.logical(flags$resume)) {
      if (flags$resume) {
        run <- "resume"
      }
    } else {
      run <- paste0("resume", " ", flags$resume)
    }
    if ("origin_run_id" %in% names(flags)) {
      run <- paste0(run, " --origin-run-id=", flags$origin_run_id)
    }
  } else {
    run <- "run"
  }

  if ("batch" %in% names(flags)) {
    if (is.logical(flags$batch)) {
      if (flags$batch) {
        batch <- "--with batch"
      } else {
        batch <- ""
      }
    } else {
      batch <- paste0("batch ", flags$batch)
      run <- ""
      if ("my_runs" %in% names(flags) && flags$my_runs) {
        batch <- paste0(batch, " --my-runs")
      }
      if ("run_id" %in% names(flags)) {
        batch <- paste0(batch, " --run-id=", flags$run_id)
      }
      if ("user" %in% names(flags)) {
        batch <- paste0(batch, " --user=", flags$user)
      }
    }
  } else {
    batch <- ""
  }

  if ("step_functions" %in% names(flags)) {
    sfn_cmd <- paste("step-functions", flags$step_functions)
    # subcommands without an argument
    for (subcommand in c("generate_new_token", 
                         "only_json", "running", "succeeded", 
                         "failed", "timed_out", "aborted")){
      if (subcommand %in% names(flags)){
        subcommand_valid <- gsub("_", "-", subcommand)
        sfn_cmd <- paste(sfn_cmd, paste0("--", subcommand_valid))
      }
    }

    # subcommands following an argument
    for (subcommand in c("authorize", "new_token", "tag", "namespace", 
                         "max_workers", "workflow_timeout")){
      if (subcommand %in% names(flags)){
        subcommand_valid <- gsub("_", "-", subcommand)
        sfn_cmd <- paste(sfn_cmd, paste0("--", subcommand_valid), flags[[subcommand]])
      }
    }
  } else {
    sfn_cmd <- ""
  }

  if ("max_workers" %in% names(flags)) {
    max_workers <- paste0("--max-workers=", flags$max_workers)
  } else {
    max_workers <- ""
  }
  if ("max_num_splits" %in% names(flags)) {
    max_num_splits <- paste0("--max-num-splits=", flags$max_num_splits)
  } else {
    max_num_splits <- ""
  }

  if ("other_args" %in% names(flags)) {
    other_args <- paste(flags$other_args)
  } else {
    other_args <- ""
  }

  parameters <- split_parameters(flags)

  if ("with" %in% names(flags)) {
    with <- unlist(lapply(seq_along(flags$with), function(x) {
      paste(paste0("--with ", unlist(flags$with[x])), collapse = " ")
    })) %>%
      paste(collapse = " ")
  } else {
    with <- ""
  }

  if ("tag" %in% names(flags)) {
    tag <- unlist(lapply(seq_along(flags$tag), function(x) {
      paste(paste0("--tag ", unlist(flags$tag[x])), collapse = " ")
    })) %>%
      paste(collapse = " ")
  } else {
    tag <- ""
  }

  if ("package_suffixes" %in% names(flags)) {
    package_suffixes <- paste0("--package-suffixes=", paste(flags$package_suffixes, collapse = ","))
  } else {
    package_suffixes <- ""
  }

  flow_RDS <- paste0("--flowRDS=", flow_file)
  cmd <- paste(
    "Rscript", run_path,
    flow_RDS,
    "--no-pylint",
    package_suffixes,
    with,
    batch,
    run,
    tag,
    parameters,
    max_workers,
    max_num_splits,
    other_args
  )

  if (batch %in% c("batch list", "batch kill")) {
    cmd <- paste("Rscript", run_path, flow_RDS, batch)
  }

  if ("logs" %in% names(flags)) {
    logs <- paste("logs", flags$logs, sep = " ")
    cmd <- paste("Rscript", run_path, flow_RDS, logs)
  }

  if ("show" %in% names(flags) && flags$show) {
    show <- "show"
    cmd <- paste("Rscript", run_path, flow_RDS, show)
  }

  if ("step_functions" %in% names(flags)){
    cmd <- paste("Rscript", run_path, flow_RDS, 
                 "--no-pylint", package_suffixes, sfn_cmd, 
                    parameters,  other_args)
  }

  if ("help" %in% names(flags) && flags$help) {
    # if help is specified by the run(...) R functions
    if ("help" %in% names(run_options) && run_options$help) {
      help_cmd <- "--help"
    } else { # if help is specified in command line
      help_cmd <- paste(commandArgs(trailingOnly = TRUE), collapse = " ")
    }
    cmd <- paste("Rscript", run_path, flow_RDS, "--no-pylint", help_cmd)
  }
  cmd
}
