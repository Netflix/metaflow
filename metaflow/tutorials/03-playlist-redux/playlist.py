from metaflow import FlowSpec, step, Parameter


class PlayListFlow(FlowSpec):
    """
    The next version of our playlist generator that uses the statistics
    generated from 'Episode 02' to improve the title recommendations.

    The flow performs the following steps:

    1) Load the genre-specific statistics from the MovieStatsFlow.
    2) In parallel branches:
       - A) Build a playlist from the top grossing films in the requested genre.
       - B) Choose a random movie.
    3) Join the two to create a movie playlist and display it.

    """

    genre = Parameter(
        "genre", help="Filter movies for a particular genre.", default="Sci-Fi"
    )

    recommendations = Parameter(
        "recommendations",
        help="The number of movies recommended for " "the playlist.",
        default=5,
    )

    @step
    def start(self):
        """
        Use the Metaflow client to retrieve the latest successful run from our
        MovieStatsFlow and assign them as data artifacts in this flow.

        """
        from metaflow import Flow, get_metadata

        # Print metadata provider
        print("Using metadata provider: %s" % get_metadata())

        # Load the analysis from the MovieStatsFlow.
        run = Flow("MovieStatsFlow").latest_successful_run
        print("Using analysis from '%s'" % str(run))

        self.genre_stats = run.data.genre_stats

        # Compute our two recommendation types in parallel.
        self.next(self.bonus_movie, self.genre_movies)

    @step
    def bonus_movie(self):
        """
        This step chooses a random title for a different movie genre.

        """
        import pandas

        # Concatenate all the genre-specific data frames and choose a random
        # movie.
        df = pandas.concat(
            [
                data["dataframe"]
                for genre, data in self.genre_stats.items()
                if genre != self.genre.lower()
            ]
        )
        df = df.sample(n=1)
        self.bonus = (df["movie_title"].values[0], df["genres"].values[0])

        self.next(self.join)

    @step
    def genre_movies(self):
        """
        Select the top performing movies from the user specified genre.

        """
        from random import shuffle

        # For the genre of interest, generate a potential playlist using only
        # highest gross box office titles (i.e. those in the last quartile).
        genre = self.genre.lower()
        if genre not in self.genre_stats:
            self.movies = []

        else:
            df = self.genre_stats[genre]["dataframe"]
            quartiles = self.genre_stats[genre]["quartiles"]
            selector = df["gross"] >= quartiles[-1]
            self.movies = list(df[selector]["movie_title"])

        # Shuffle the playlist.
        shuffle(self.movies)

        self.next(self.join)

    @step
    def join(self, inputs):
        """
        Join our parallel branches and merge results.

        """
        self.playlist = inputs.genre_movies.movies
        self.bonus = inputs.bonus_movie.bonus

        self.next(self.end)

    @step
    def end(self):
        """
        Print out the playlist and bonus movie.

        """
        # Print the playlist.
        print("Playlist for movies in genre '%s'" % self.genre)
        for pick, movie in enumerate(self.playlist, start=1):
            print("Pick %d: '%s'" % (pick, movie))
            if pick >= self.recommendations:
                break

        print("Bonus Pick: '%s' from '%s'" % (self.bonus[0], self.bonus[1]))


if __name__ == "__main__":
    PlayListFlow()
