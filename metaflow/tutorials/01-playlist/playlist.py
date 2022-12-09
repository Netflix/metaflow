from metaflow import FlowSpec, step, IncludeFile, Parameter


def script_path(filename):
    """
    A convenience function to get the absolute path to a file in this
    tutorial's directory. This allows the tutorial to be launched from any
    directory.

    """
    import os

    filepath = os.path.join(os.path.dirname(__file__))
    return os.path.join(filepath, filename)


class PlayListFlow(FlowSpec):
    """
    A flow to help you build your favorite movie playlist.

    The flow performs the following steps:
    1) Ingests a CSV file containing metadata about movies.
    2) Loads two of the columns from the CSV into python lists.
    3) In parallel branches:
       - A) Filters movies by the genre parameter.
       - B) Choose a random movie from a different genre.
    4) Displays the top entries from the playlist.

    """

    movie_data = IncludeFile(
        "movie_data",
        help="The path to a movie metadata file.",
        default=script_path("movies.csv"),
    )

    genre = Parameter(
        "genre", help="Filter movies for a particular genre.", default="Sci-Fi"
    )

    recommendations = Parameter(
        "recommendations",
        help="The number of movies to recommend in " "the playlist.",
        default=5,
    )

    @step
    def start(self):
        """
        Parse the CSV file and load the values into a dictionary of lists.

        """
        # For this example, we only need the movie title and the genres.
        columns = ["movie_title", "genres"]

        # Create a simple data frame as a dictionary of lists.
        self.dataframe = dict((column, list()) for column in columns)

        # Parse the CSV header.
        lines = self.movie_data.split("\n")
        header = lines[0].split(",")
        idx = {column: header.index(column) for column in columns}

        # Populate our dataframe from the lines of the CSV file.
        for line in lines[1:]:
            if not line:
                continue

            fields = line.rsplit(",", 4)
            for column in columns:
                self.dataframe[column].append(fields[idx[column]])

        # Compute genre-specific movies and a bonus movie in parallel.
        self.next(self.bonus_movie, self.genre_movies)

    @step
    def bonus_movie(self):
        """
        This step chooses a random movie from a different genre.

        """
        from random import choice

        # Find all the movies that are not in the provided genre.
        movies = [
            (movie, genres)
            for movie, genres in zip(
                self.dataframe["movie_title"], self.dataframe["genres"]
            )
            if self.genre.lower() not in genres.lower()
        ]

        # Choose one randomly.
        self.bonus = choice(movies)

        self.next(self.join)

    @step
    def genre_movies(self):
        """
        Filter the movies by genre.

        """
        from random import shuffle

        # Find all the movies titles in the specified genre.
        self.movies = [
            movie
            for movie, genres in zip(
                self.dataframe["movie_title"], self.dataframe["genres"]
            )
            if self.genre.lower() in genres.lower()
        ]

        # Randomize the title names.
        shuffle(self.movies)

        self.next(self.join)

    @step
    def join(self, inputs):
        """
        Join our parallel branches and merge results.

        """
        # Reassign relevant variables from our branches.
        self.playlist = inputs.genre_movies.movies
        self.bonus = inputs.bonus_movie.bonus

        self.next(self.end)

    @step
    def end(self):
        """
        Print out the playlist and bonus movie.

        """
        print("Playlist for movies in genre '%s'" % self.genre)
        for pick, movie in enumerate(self.playlist, start=1):
            print("Pick %d: '%s'" % (pick, movie))
            if pick >= self.recommendations:
                break

        print("Bonus Pick: '%s' from '%s'" % (self.bonus[0], self.bonus[1]))


if __name__ == "__main__":
    PlayListFlow()
