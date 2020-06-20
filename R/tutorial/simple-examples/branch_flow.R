library(metaflow)

a <- function(self){
    self$var = 1
}

b <- function(self){
    self$var = 2
}

join <- function(self, inputs){
    print(paste("var in step a is", inputs[[1]]$var))
    print(paste("var in step b is", inputs[[2]]$var))
}

metaflow("BranchFlow") %>%
    step(step = "start",
         next_step = c("a", "b")) %>%
    step(step = "a", 
         r_function=a, 
         next_step="join") %>%
    step(step="b", 
         r_function=b, 
         next_step="join") %>%
    step(step="join", 
         r_function=join, 
         next_step="end",
         join=TRUE) %>%
    step(step="end") %>%
    run()