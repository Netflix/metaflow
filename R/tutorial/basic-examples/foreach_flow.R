library(metaflow)

start <- function(self){
    self$params= c("param1", "param2", "param3") 
}

a <- function(self){
    self$result = paste(self$input, "processed")
}

join <- function(self, inputs){
    results <- gather_inputs(inputs, "result")
    print(results)
}

metaflow("ForeachFlow") %>%
    step(step = "start",
         r_function = start,
         next_step = "a",
         foreach="params") %>%
    step(step = "a", 
         r_function=a, 
         next_step="join") %>%
    step(step="join", 
         r_function=join, 
         next_step="end",
         join=TRUE) %>%
    step(step="end") %>%
    run()