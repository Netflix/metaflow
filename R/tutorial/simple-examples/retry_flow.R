library(metaflow)

start <- function(self){
    n <- rbinom(n=1, size=1, prob=0.5)
    if (n == 0){
        stop("Bad Luck!") 
    } else{
        print("Lucky you!")
    }
}

end <- function(self){
    print("Phew!")
}

metaflow("RetryFlow") %>%
    step(step="start", 
         decorator("retry", times=3),
         r_function=start, 
         next_step="end") %>%
    step(step="end", 
         r_function=end) %>% 
    run()