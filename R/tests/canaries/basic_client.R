library(metaflow)

common_properties <- function(obj){
    cat(paste("tags: ", paste(obj$tags, collapse=", "), "\n"))
    cat(paste("created_at: ", obj$created_at, "\n"))
    cat(paste("parent: ", obj$parent, "\n"))
    cat(paste("pathspec: ", obj$pathspec, "\n"))
}

flow_properties <- function(flow){
    cat("\n=======flow object properties======\n")
    common_properties(flow)
    cat(paste("latest_run: ", flow$latest_run, "\n"))
    cat(paste("latest_successful_run: ", flow$latest_successful_run, "\n"))
    cat(paste("run: ", paste(flow$runs, collapse=", "), "\n"))
    flow$summary()
}

run_properties <- function(run){
    cat("\n=======run object properties======\n")
    common_properties(run)

    #cat(paste("code: ", run$code, "\n"))
    cat(paste("end_task: ", run$end_task, "\n"))
    cat(paste("artifacts: ", run$artifacts, "\n"))
    cat(paste("finished: ", run$finished, "\n"))
    cat(paste("successful: ", run$successful, "\n"))
    cat(paste("steps: ", run$steps, "\n"))
    cat(paste("artifacts: ", run$artifacts, "\n"))
    run$summary()
}

step_properties <- function(step) {
    cat("\n=======step object properties======\n")
    common_properties(step)

    cat(paste("a_task: ", step$a_task, "\n"))
    cat(paste("tasks: ", step$tasks, "\n"))
    step$summary()
}

task_properties <- function(task){
    cat("\n=======task object properties======\n")
    common_properties(task) 

    cat(paste("exception: ", task$exception, "\n"))
    #cat(paste("code: ", task$code, "\n"))
    cat(paste("index: ", task$index, "\n"))
    cat(paste("metadata_dict: ", task$metadata_dict, "\n"))
    cat(paste("runtime_name: ", task$runtime_name, "\n"))
    cat(paste("stderr: ", task$stderr, "\n"))
    cat(paste("stdout: ", task$stdout, "\n"))
    cat(paste("successful: ", task$successful, "\n"))
    cat(paste("artifacts: ", task$artifacts, "\n"))
    task$artifact(task_artifacts)

    task$summary()
}

set_metadata('/Users/jge/github/metaflow/R/tests/canaries')
flow <- flow_client$new("BasicHelloWorldFlow")

run <- run_client$new(flow, flow$latest_successful_run)

step <- step_client$new(run, "start")

task <- task_client$new(step, step$a_task)


flow_properties(flow)
run_properties(run)
step_properties(step)
task_properties(task)

