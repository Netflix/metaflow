#  A flow where Metaflow prints 'Hi'.
#  Run this flow to validate that Metaflow is installed correctly.

library(metaflow)

# This is the 'start' step. All flows must have a step named 
# 'start' that is the first step in the flow.
start <- function(self){
    print("HelloFlow is starting.")
}

# A step for metaflow to introduce itself.
hello <- function(self){
    print("Metaflow says: Hi!") 
}

# This is the 'end' step. All flows must have an 'end' step, 
# which is the last step in the flow.
end <- function(self){
     print("HelloFlow is all done.")
}

metaflow("HelloFlow") %>%
    step(step = "start", 
         r_function = start, 
         next_step = "hello") %>%
    step(step = "hello", 
         r_function = hello,  
         next_step = "end") %>%
    step(step = "end", 
         r_function = end) %>% 
    run()
