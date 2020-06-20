library(metaflow)

start <- function(self){
    print(paste0("flow name: ", current("flow_name")))
    print(paste0("run id: ", current("run_id")))
    print(paste0("origin run id: ", current("origin_run_id")))
    print(paste0("step name: ", current("step_name")))
    print(paste0("task id: ", current("task_id")))
    print(paste0("pathspec: ", current("pathspec")))
    print(paste0("username: ", current("username")))
}

metaflow("CurrentFlow") %>%
    step(step="start", 
         r_function=start, 
         next_step="end") %>%
    step(step="end") %>% 
    run()