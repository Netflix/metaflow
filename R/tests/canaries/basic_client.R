library(metaflow)

client <- mf_client$new()
f <- client$flow("BasicHelloWorldFlow")
f$summary()

run_id <- f$latest_successful_run
r <- client$run(f, run_id)
r$summary()

print(r$tasks)
#task_id <- r$end_task
#print(task_id)
#t <- client$task(r, task_id)
#t$summary()