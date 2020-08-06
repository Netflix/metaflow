library(metaflow)

# The start step:
start <- function(self){
    # Loads the movie data into a data frame
    self$df <- read.csv("./movies.csv", stringsAsFactors=FALSE)

    # find all unique genres
    self$genres <- levels(as.factor(self$df$genre))
}

# Compute statistics for a single genre.
compute_stats <- function(self){
    self$genre <- self$input
    message("Computing statistics for ", self$genre)

    # Find all the movies that have this genre 
    self$df_by_genre <- self$df[self$df$genre == self$genre, ]

    gross <- self$df_by_genre$gross

    # Get some statistics on the gross box office for these titles.
    self$median <- median(gross) 
    self$mean <- mean(gross)
}

#  Join our parallel branches and merge results into a data frame.
join <- function(self, inputs){
    self$stats <- data.frame(
        "genres" = unlist(lapply(inputs, function(inp){inp$genre})),
        "median" = unlist(lapply(inputs, function(inp){inp$median})),
        "mean" = unlist(lapply(inputs, function(inp){inp$mean})))
    
    print(head(self$stats))
}

metaflow("MovieStatsFlow") %>%
    step(step = "start",
          r_function = start,
          next_step = "compute_stats",
          foreach = "genres") %>%
    step(step = "compute_stats",
         r_function = compute_stats,
         next_step = "join") %>%
    step(step = "join",
         r_function = join,
         next_step = "end",
         join = TRUE) %>%
    step(step = "end") %>%
    run()
    

