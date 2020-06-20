library(metaflow)

start <- function(self){
    self$pass_down <- "non-modified"
}

a <- function(self){
    self$common <- "common in a and b"
    self$x <- "x in a" 
    self$y <- "y in a" 
    self$from_a <- "only in a" 
}

b <- function(self){
    self$common <- "common in a and b" 
    self$x <- "x in b" 
    self$y <- "y in b" 
}

join <- function(self, inputs){
    # manually propogate variable that has different values in different branches 
    self$x <- inputs[[1]]$x

    merge_artifacts(self, inputs, exclude=list("y"))

    # If without the merge_artifact, the following artifact access won't work. 
    print(paste('pass_down is', self$pass_down))
    print(paste('from_a is', self$from_a))
    print(paste('common is', self$common))
}

metaflow("BranchFlow") %>%
    step(step = "start",
         r_function=start,
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