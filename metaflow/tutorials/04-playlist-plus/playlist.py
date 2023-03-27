from metaflow import FlowSpec, step, Parameter, conda, conda_base


def get_python_version():
    """
    A convenience function to get the python version used to run this
    tutorial. This ensures that the conda environment is created with an
    available version of python.

    """
    import platform

    versions = {"2": "2.7.15", "3": "3.9.10"}
    return versions[platform.python_version_tuple()[0]]


# Use the specified version of python for this flow.
@conda_base(python=get_python_version())
class PlayListFlow(FlowSpec):
    """
    The next version of our playlist generator that adds a 'hint' parameter to
    choose a bonus movie closest to the 'hint'.

    The flow performs the following steps:

    1) Load the genre-specific statistics from the MovieStatsFlow.
    2) In parallel branches:
       - A) Build a playlist from the top films in the requested genre.
       - B) Choose a bonus movie that has the closest string edit distance to
         the user supplied hint.
    3) Join the two to create a movie playlist and display it.

    """

    genre = Parameter(
        "genre", help="Filter movies for a particular genre.", default="Sci-Fi"
    )

    hint = Parameter(
        "hint",
        help="Give a hint to the bonus movie algorithm.",
        default="Metaflow Release",
    )

    recommendations = Parameter(
        "recommendations",
        help="The number of movies recommended for the playlist.",
        default=5,
    )

    @step
    def start(self):
        """
        Use the Metaflow client to retrieve the latest successful run from our
        MovieStatsFlow and assign them as data artifacts in this flow.

        """
        # Load the analysis from the MovieStatsFlow.
        from metaflow import Flow, get_metadata

        # Print metadata provider
        print("Using metadata provider: %s" % get_metadata())

        # Load the analysis from the MovieStatsFlow.
        run = Flow("MovieStatsFlow").latest_successful_run
        print("Using analysis from '%s'" % str(run))

        # Get the dataframe from the start step before we sliced into into
        # genre-specific dataframes.
        self.dataframe = run["start"].task.data.dataframe

        # Also grab the summary statistics.
        self.genre_stats = run.data.genre_stats

        # Compute our two recommendation types in parallel.
        self.next(self.bonus_movie, self.genre_movies)

    @conda(libraries={"editdistance": "0.5.3"})
    @step
    def bonus_movie(self):
        """
        Use the user supplied 'hint' argument to choose a bonus movie that has
        the closest string edit distance to the hint.

        This step uses 'conda' to isolate the environment. Note that the
        package 'editdistance' need not be installed in your python
        environment.
        """
        import editdistance

        # Define a helper function to compute the similarity between two
        # strings.
        def _edit_distance(movie_title):
            return editdistance.eval(self.hint, movie_title)

        # Compute the distance and take the argmin to find the closest title.
        distance = [
            _edit_distance(movie_title) for movie_title in self.dataframe["movie_title"]
        ]
        index = distance.index(min(distance))
        self.bonus = (
            self.dataframe["movie_title"][index],
            self.dataframe["genres"][index],
        )

        self.next(self.join)

    @step
    def genre_movies(self):
        """
        Select the top performing movies from the use specified genre.
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
            self.movies = [
                df["movie_title"][i]
                for i, g in enumerate(df["gross"])
                if g >= quartiles[-1]
            ]

        # Shuffle the content.
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
        This step simply prints out the playlist.

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
