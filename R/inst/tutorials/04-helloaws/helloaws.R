#  A flow where Metaflow prints 'Hi'.
#  Run this flow to validate that Metaflow is installed correctly.

library(metaflow)

# This is the 'start' step. All flows must have a step named 
# 'start' that is the first step in the flow.
start <- function(self){
    message("HelloAWS is starting.")
    message("Using metadata provider: ", get_metadata())
}

# A step for metaflow to introduce itself.
hello <- function(self){
    self$message <- "We're on the cloud! Metaflow says: Hi!"
    print(self$message) 
    message("Using metadata provider: ", get_metadata())
}

# This is the 'end' step. All flows must have an 'end' step, 
# which is the last step in the flow.
end <- function(self){
    message("HelloAWS is all done.")
}

metaflow("HelloAWSFlow") %>%
    step(step = "start", 
         r_function = start, 
         next_step = "hello") %>%
    step(step = "hello", 
         decorator("retry", times=2),
         decorator("batch", cpu=2, memory=2048),
         r_function = hello,  
         next_step = "end") %>%
    step(step = "end", 
         r_function = end) %>% 
    run()
