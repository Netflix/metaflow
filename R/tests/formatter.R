node_quals <- function(name, node) {
  node_quals <- c("all")
  if ("quals" %in% names(node)) {
    node_quals <- c(node_quals, node$quals)
  }
  if (name %in% c("start", "end")) {
    node_quals <- c(node_quals, name)
  }
  if ("join" %in% names(node)) {
    node_quals <- c(node_quals, "join")
  }
  if ("linear" %in% names(node)) {
    node_quals <- c(node_quals, "linear")
  }
  return(node_quals)
}

get_step_func_names <- function(test) {
  is_step <- function(func_name) {
    step <- test[[func_name]]
    return("type" %in% names(attributes(step)) && attr(step, "type") == "step")
  }

  func_names <- Filter(is_step, names(test))
}

choose_step <- function(test, name, node, graph_name) {
  func_names <- get_step_func_names(test)
  prio <- sapply(func_names, function(name) {
    attr(test[[name]], "prio")
  })
  func_names <- func_names[order(prio)]

  quals <- node_quals(name, node)
  for (func_name in func_names) {
    step <- test[[func_name]]
    step_quals <- attr(step, "quals")

    if (length(intersect(step_quals, quals)) > 0) {
      return(list(name = func_name, func = step))
    }
  }
  return(NULL)
}

flow_lines <- function(graphspec, test, context) {
  # graph: a json object parsed from rjson library
  # test: an environment containing test functions & check functions
  lines <- c("# -*- coding: utf-8 -*-")

  lines <- c(lines, "library(metaflow)")

  used_test_func <- c()

  for (name in names(graphspec$graph)) {
    node <- graphspec$graph[[name]]

    if ("join" %in% names(node)) {
      lines <- c(lines, sprintf("%s <- function(self, inputs){", name))
    } else {
      lines <- c(lines, sprintf("%s <- function(self){", name))
    }

    if ("foreach" %in% names(node)) {
      lines <- c(lines, sprintf(
        "self$%s <- %s", node$foreach_var,
        node$foreach_var_default
      ))
    }

    step <- choose_step(test, name, node)

    if (is.null(step)) {
      stop(paste(
        "Test", test$name, "does not have a match for step",
        name, "in graph", graphspec$name
      ))
    }

    step_func <- step$func
    used_test_func <- c(used_test_func, step$name)

    step_body <- deparse(body(step_func))

    # ignore the { and } on the first & last lines
    if (length(step_body) > 2) {
      lines <- c(lines, step_body[2:(length(step_body) - 1)])
    }
    lines <- c(lines, "}")
    lines <- c(lines, "")
  }

  func_names <- get_step_func_names(test)
  func_names <- Filter(function(name) {
    attr(test[[name]], "required")
  }, func_names)
  for (func_name in func_names) {
    step <- test[[func_name]]
    if (attr(step, "required") && !(func_name %in% used_test_func)) {
      stop(sprintf(
        "Test %s requires function %s but it was not matched for graph %s",
        test$name, func_name, graphspec$name
      ))
    }
  }

  flow_name <- sprintf("%sFlow", test$name)
  lines <- c(lines, sprintf('test_flow <- metaflow("%s") %%>%%', flow_name))

  if ("parameters" %in% names(test)) {
    for (par_name in names(test$parameters)) {
      par <- test$parameters[[par_name]]
      par_items <- c()
      for (name in names(par)) {
        par_items <- c(par_items, sprintf("%s=%s", name, par[[name]]))
      }
      par_items_str <- paste(par_items, collapse = ",")
      if (length(par_items) > 0) {
        lines <- c(lines, sprintf("  parameter('%s',%s) %%>%% ", par_name, par_items))
      } else {
        lines <- c(lines, sprintf("  parameter('%s') %%>%%", par_name))
      }
    }
  }

  for (name in names(graphspec$graph)) {
    node <- graphspec$graph[[name]]

    lines <- c(lines, "step(")
    lines <- c(lines, sprintf('  step = "%s",', name))
    lines <- c(lines, sprintf("  r_function = %s,", name))

    if ("foreach" %in% names(node)) {
      lines <- c(lines, sprintf('  foreach = "%s",', node$foreach_var))
    } else {
      lines <- c(lines, "  foreach = NULL,")
    }

    if ("linear" %in% names(node)) {
      lines <- c(lines, sprintf('  next_step = "%s",', node$linear))
    } else if ("branch" %in% names(node)) {
      branches <- paste(lapply(
        node$branch,
        function(name) {
          sprintf('"%s"', name)
        }
      ), collapse = ",")
      lines <- c(lines, sprintf("  next_step = c(%s),", branches))
    } else if ("foreach" %in% names(node)) {
      lines <- c(lines, sprintf('  next_step = "%s",', node$foreach))
    }

    if ("join" %in% names(node)) {
      lines <- c(lines, "  join = TRUE")
    } else {
      lines <- c(lines, "  join = FALSE")
    }

    if (name == "end") {
      lines <- c(lines, ")")
    } else {
      lines <- c(lines, ") %>%")
    }
  }
  lines <- c(lines, "")
  top_options <- paste(context$top_options, collapse = ", ")
  lines <- c(lines, sprintf("status_code <- test_flow %%>%% run(%s)", top_options))
  return(lines)
}

fetch_artifact <- function(checker, step_id = "end", var_name = "data") {
  client <- mf_client$new()
  test_flow <- client$flow(checker$flow_name)
  run_id <- test_flow$latest_successful_run
  test_run <- test_flow$run(run_id)
  test_step <- test_run$step(step_id)
  test_task <- test_step$task(test_step$tasks[1])
  test_task$artifact(var_name)
}

parse_function <- function(f, fname) {
  fargs <- c()
  for (name in names(formals(f))) {
    if (typeof(formals(f)[[name]]) == "symbol") {
      fargs <- c(fargs, name)
    } else {
      fargs <- c(fargs, sprintf("%s = %s", name, deparse(formals(f)[[name]])))
    }
  }

  lines <- c(sprintf("%s <- function(%s)", fname, paste(fargs, collapse = ",")))
  for (line in deparse(body(f))) {
    lines <- c(lines, line)
  }
  return(lines)
}

check_lines <- function(test) {
  lines <- c()
  lines <- c(lines, "# -*- coding: utf-8 -*-")
  lines <- c(lines, "library(metaflow)")
  lines <- c(lines, "")

  for (line in parse_function(fetch_artifact, "fetch_artifact")) {
    lines <- c(lines, line)
  }

  lines <- c(lines, "")

  is_check <- function(func_name) {
    step <- test[[func_name]]
    return("type" %in% names(attributes(step)) && attr(step, "type") == "check")
  }

  func_names <- Filter(is_check, names(test))
  for (func_name in func_names) {
    check <- test[[func_name]]
    for (line in parse_function(check, func_name)) {
      lines <- c(lines, line)
    }
  }

  flow_name <- sprintf("%sFlow", test$name)
  lines <- c(lines, sprintf('checker <- list(flow_name = "%s")', flow_name))
  lines <- c(lines, sprintf("client <- mf_client$new()"))
  lines <- c(lines, sprintf('test_flow <- client$flow("%s")', flow_name))

  for (func_name in func_names) {
    lines <- c(lines, sprintf('get("%s")(checker, test_flow)', func_name))
  }

  return(lines)
}
