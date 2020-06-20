library(metaflow)

start <- function(self){
    self$my_var <- "hello world"
}

a <- function(self){
    print(paste("the data artifact is", self$my_var))
}

end <- function(self){
    print("End of the linear flow")
}

metaflow("LinearFlow") %>%
    step(step="start", 
         r_function=start, 
         next_step="a") %>%
    step(step="a", 
         r_function=a,  
         next_step="end") %>%
    step(step="end", 
         r_function=end) %>% 
    run()
