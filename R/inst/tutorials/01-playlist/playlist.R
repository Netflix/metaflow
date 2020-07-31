# A flow to help you build your favorite movie playlist.

# The flow performs the following steps:
#  1) Ingests a CSV file containing metadata about movies.
#  2) Loads two of the columns from the CSV into python lists.
#  3) In parallel branches:
#     - A) Filters movies by the genre parameter.
#     - B) Choose a random movie from a different genre.
#  4) Displays the top entries from the playlist.

library(metaflow)

# Parse the CSV file 
start <- function(self){
    self$df <- read.csv("./movies.csv", stringsAsFactors=FALSE)
}

# Filter the movies by genre.
pick_movie <- function(self){
    # select rows which has the specified genre
    movie_by_genre <- self$df[self$df$genre == self$genre, ]

    # randomize the title names
    shuffled_rows <- sample(nrow(movie_by_genre))
    self$movies <- movie_by_genre[shuffled_rows, ]
}

# This step chooses a random movie from a different genre.
bonus_movie <- function(self){
    # select all movies not matching the specified genre
    bonus_movies <- self$df[self$df$genre != self$genre, ]

    idx <- sample(nrow(bonus_movies), size=1)
    self$bonus <- bonus_movies$movie_title[idx]
}

#  Join our parallel branches and merge results.
join <- function(self, inputs){
    # Reassign relevant variables from our branches.
    self$bonus <- inputs$bonus_movie$bonus
    self$playlist <- inputs$pick_movie$movies
}

# Print out the playlist and bonus movie.
end <- function(self){
    message("Playlist for movies in genre: ", self$genre)
    print(head(self$playlist))
    for (i in 1:nrow(self$playlist)){
        message(sprintf("Pick %d: %s", i, self$playlist$movie_title[i]))

        if (i >= self$top_k) break; 
    }
}

metaflow("PlayListFlow") %>% 
    parameter("genre", 
              help = "Filter movies for a particular genre.", 
              default = "Sci-Fi") %>%    
    parameter("top_k",
              help = "The number of movies to recommend in the playlist.",
              default = 5,
              type = "int") %>%
    step(step = "start", 
         r_function = start, 
         next_step = c("pick_movie", "bonus_movie")) %>%
    step(step = "pick_movie",
         r_function = pick_movie,
         next_step = "join") %>%
    step(step = "bonus_movie",
         r_function = bonus_movie,
         next_step = "join") %>%
    step(step = "join",
         r_function = join,
         join = TRUE,
         next_step = "end") %>%
    step(step = "end", 
         r_function = end) %>%
    run()

