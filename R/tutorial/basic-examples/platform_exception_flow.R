library(metaflow)

start <- function(self){
    print("this runs on batch") 
}

end <- function(self){
    if (!is.null(self$start_failed)){
        print("The previous step failed to start")
    } else {
        print("Phew!")
    }
}

metaflow("PlatformExceptionFlow") %>%
    step(step="start", 
         decorator("batch", cpu=100),
         decorator("retry", times=3),
         decorator("catch", var="start_failed"),
         r_function=start, 
         next_step="end") %>%
    step(step="end", 
         r_function=end) %>% 
    run()