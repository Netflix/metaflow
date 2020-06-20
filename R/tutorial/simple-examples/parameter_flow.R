library(metaflow)

start <- function(self){
    print(paste("alpha is", self$alpha))
}

end <- function(self){
    print(paste("alpha still is", self$alpha))
}

metaflow("ParameterFlow") %>%
    parameter("alpha", 
              help="learning rate", 
              required = TRUE,
              type = "bool") %>%    
    step(step="start", 
         r_function=start, 
         next_step="end") %>%
    step(step="end", 
         r_function=end) %>% 
    run()