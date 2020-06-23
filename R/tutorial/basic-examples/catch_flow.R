library(metaflow)

start <- function(self){
    self$params <- c(-1, 2, 3) 
}

sanity_check <- function(self){
    print(self$input)
    if (self$input < 0) {
        stop("input cannot be negative")
    }
}

join <- function(self, inputs){
    for (input in inputs){
        if (!is.null(input$compute_failed)){
           print(paste0("Exception happened for param: ", input$input))
           print("Exception message:")
           print(input$compute_failed)
        }
    }
}

metaflow("CatchFlow") %>%
    step(step = "start",
         r_function = start,
         next_step = "sanity_check",
         foreach = "params") %>%
    step(step = "sanity_check", 
         decorator("catch", var="compute_failed", print_exception=FALSE),
         r_function = sanity_check, 
         next_step = "join") %>%
    step(step = "join", 
         r_function = join, 
         next_step = "end",
         join = TRUE) %>%
    step(step = "end") %>%
    run()