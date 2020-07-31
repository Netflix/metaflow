# Episode 03-playlist-redux: Follow the Money.

**Use Metaflow to load the statistics generated from 'Episode 02' and recommend movies from a genre with highest median gross box office**

#### Showcasing:
- Using data artifacts generated from other flows.

#### Before playing this episode:
1. Run 'Episode 02-statistics: Is this Data Science?'
2. Configure your metadata provider to a user-wise global provider, if you haven't done it already. 
```bash
$mkdir -p /path/to/home/.metaflow
$export METAFLOW_DEFAULT_METADATA=local
```

#### To play this episode:
1. ```cd metaflow-tutorials/R```
2. ```Rscript 03-playlist-redux/playlist.R show```
3. ```Rscript 03-playlist-redux/playlist.R run```

In this ```PlayListReduxFlow```, we reuse the genre median gross box office statistics computed from ```MoviesStatsFlow```, pick the genre with the highest median gross box office, and create a randomized playlist of movies of this picked genre.

```R
library(metaflow)

#  Use the Metaflow client to retrieve the latest successful run from our
#  MovieStatsFlow and assign them as data artifacts in this flow.
start <- function(self){
    # Loads the movie data into a data frame
    self$df <- read.csv("./movies.csv", stringsAsFactors=FALSE)

    message("Using metadata provider: ", get_metadata())

    flow <- flow_client$new("MovieStatsFlow")
    run <- run_client$new(flow, flow$latest_successful_run)
    message("Using analysis from: ", run$pathspec)

    self$genre_stats <- run$artifact("stats")
}

# Pick some movies from the genre with highest median gross box office 
# which we calculated in MovieStatsFlow
pick_movie <- function(self){
    sort_order <- order(self$genre_stats$median, decreasing=TRUE)
    sorted_stats <- self$genre_stats[sort_order, ]

    self$picked_genre <- sorted_stats$genres[1]

    message("Picked genre: ", self$picked_genre, " with the highest median gross box office.")

    # generate a randomized playlist of titles of the picked genre
    movie_by_genre <- self$df[self$df$genre == self$picked_genre, ]
    shuffled_rows <- sample(nrow(movie_by_genre))
    self$playlist <- movie_by_genre[shuffled_rows, ]
}

# Print out the picked movies
end <- function(self){
    message("Playlist for movies in picked genre: ", self$picked_genre)
    for (i in 1:nrow(self$playlist)){
        message(sprintf("Pick %d: %s", i, self$playlist$movie_title[i]))

        if (i >= self$top_k) break; 
    }
}

metaflow("PlayListReduxFlow") %>%
    parameter("top_k",
              help = "The number of movies to recommend in the playlist.",
              default = 5,
              type = "int") %>%
    step(step = "start", 
         r_function = start, 
         next_step = "pick_movie") %>%
    step(step = "pick_movie",
         r_function = pick_movie,
         next_step = "end") %>%
    step(step = "end", 
         r_function = end) %>%
    run()
```