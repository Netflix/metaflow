flags <- function(...) {
  flags <- list(...)
  config <- parse_flags()
  flags <- flags[!names(flags) %in% names(config)]
  c(flags, config)
}

parse_flags <- function(arguments = commandArgs(TRUE)) {
  config_name <- Sys.getenv("R_CONFIG_ACTIVE", unset = "default")

  configs <- pkg.env$configs 
  loaded_configs <- list()
  for (key in names(configs[[config_name]])) {
    loaded_configs[[key]] <- eval(configs[[config_name]][[key]])
  }

  return(append(loaded_configs, parse_arguments(arguments)))
}

parse_arguments <- function(arguments = NULL) {
  # if arguments are null look for commandArgs
  if (is.null(arguments)) {
    arguments <- commandArgs(TRUE)
  }
  arguments <- split_flags(arguments)
  values <- list()
  i <- 0
  n <- length(arguments)
  while (i < n) {
    i <- i + 1
    argument <- arguments[[i]]
    if (argument == "resume") {
      if (i + 1 <= n && !grepl("^--", arguments[[i + 1]])) {
        values$resume <- arguments[[i + 1]]
        i <- i + 1
      } else {
        values$resume <- TRUE
      }
      next
    }

    if (argument == "step-functions"){
      i <- i + 1
      if (i <= n){
        values$step_functions <- arguments[i]
      } else {
        values$step_functions <- "" 
      }
      next
    }

    if (!grepl("^--", argument)) {
      if (grepl("batch", argument)) {
        values$batch <- parse_batch(arguments)
        next
      }
      if (grepl("show", argument)) {
        values$show <- TRUE
        next
      }
      if (grepl("logs", argument)) {
        values$logs <- parse_logs(arguments)
        next
      }
      if (grepl("help", argument)) {
        values$help <- TRUE
        next
      }
      next
    } else {
      if (grepl("--package-suffixes", argument)) {
        package_suffixes <- arguments[grepl("\\.", arguments)]
        package_suffixes <- gsub("--package-suffixes", "", package_suffixes)
        package_suffixes <- gsub("=", "", package_suffixes)
        values$package_suffixes <- paste(package_suffixes, collapse = "")
        next
      }
      if (grepl("--with", argument)) {
        values$with <- c(values$with, arguments[[i + 1]])
        i <- i + 1
        next
      }
      if (grepl("--tag", argument)) {
        values$tag <- c(values$tag, arguments[[i + 1]])
        i <- i + 1
        next
      }

      # parse parameters for example
      # Rscript flow.R run --lr 0.01 --name "test" --flag
      # currently support numeric type / string type / boolean flag
      equals_idx <- regexpr("=", argument)
      if (identical(c(equals_idx), -1L)) {
        key <- substring(argument, 3)
        if (i + 1 <= n && !grepl("^--", arguments[[i + 1]])) {
          val <- arguments[[i + 1]]
          i <- i + 1
        } else {
          val <- TRUE
        }
      } else {
        key <- substring(argument, 3, equals_idx - 1)
        val <- substring(argument, equals_idx + 1)
      }
      key <- gsub("-", "_", key)
      values[[key]] <- val
    }
  }
  values
}

parse_logs <- function(arguments) {
  no_prefix <- arguments[!grepl("^--", arguments)]
  logs <- which(no_prefix == "logs")
  logs_arg <- no_prefix[logs + 1]
  if (length(logs_arg) == 1) {
    paste(logs_arg, collapse = " ")
  }
}

parse_batch <- function(arguments) {
  no_prefix <- arguments[!grepl("^--", arguments)]
  batch <- which(no_prefix == "batch")
  batch_arg <- no_prefix[batch + 1]
  if (length(batch_arg) == 1) {
    paste(batch_arg, collapse = " ")
  }
}


split_flags <- function(arguments) {
  lapply(arguments, function(x) {
    strsplit(x, split = " ")[[1]]
  }) %>%
    unlist()
}

split_parameters <- function(flags) {
  parameters <- !names(flags) %in% c(
    "metaflow_path", "run",
    "batch", "datastore", "metadata",
    "package_suffixes", "no-pylint",
    "help", "resume",
    "max_num_splits", "max_workers",
    "other_args", "show", "user",
    "my_runs", "run_id", 
    "origin_run_id", "with", "tag",
    # step-functions subcommands and options
    "step_functions", 
    "only_json", "generate_new_token",
    "running", "succeeded", "failed", 
    "timed_out", "aborted", "namespace",
    "new_token", "workflow_timeout"
  )
  parameters <- flags[parameters]
  if (length(parameters) == 0) {
    valid_params <- ""
  } else {
    valid_params <- unlist(lapply(seq_along(parameters), function(x) {
      paste(paste0("--", names(parameters[x]), " ", unlist(parameters[x])), collapse = " ")
    })) %>%
      paste(collapse = " ")
  }
  valid_params <- gsub("_", "-", valid_params)
  valid_params
}
