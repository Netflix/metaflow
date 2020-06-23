library(metaflow)

start <- function(self){
    n <- 10000 
    # 800MB artifact
    self$large <- matrix(1:n*n, nrow=n, ncol=n)
}

end <- function(self){
    print(sum(self$large))
}

metaflow("BigSumFlow") %>%
    step(step="start", 
         decorator("resources", cpu=4, memory=4096),
         r_function=start, 
         next_step="end") %>%
    step(step="end", 
         decorator("resources", cpu=4, memory=4096),
         r_function=end) %>% 
    run(batch=TRUE)