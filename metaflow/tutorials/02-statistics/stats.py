from metaflow import FlowSpec, step, IncludeFile


def script_path(filename):
    """
    A convenience function to get the absolute path to a file in this
    tutorial's directory. This allows the tutorial to be launched from any
    directory.

    """
    import os

    filepath = os.path.join(os.path.dirname(__file__))
    return os.path.join(filepath, filename)


class MovieStatsFlow(FlowSpec):
    """
    A flow to generate some statistics about the movie genres.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Fan-out over genre using Metaflow foreach.
    3) Compute quartiles for each genre.
    4) Save a dictionary of genre-specific statistics.

    """

    movie_data = IncludeFile(
        "movie_data",
        help="The path to a movie metadata file.",
        default=script_path("movies.csv"),
    )

    @step
    def start(self):
        """
        The start step:
        1) Loads the movie metadata into pandas dataframe.
        2) Finds all the unique genres.
        3) Launches parallel statistics computation for each genre.

        """
        import pandas
        from io import StringIO

        # Load the data set into a pandas dataframe.
        self.dataframe = pandas.read_csv(StringIO(self.movie_data))

        # The column 'genres' has a list of genres for each movie. Let's get
        # all the unique genres.
        self.genres = {
            genre for genres in self.dataframe["genres"] for genre in genres.split("|")
        }
        self.genres = list(self.genres)

        # We want to compute some statistics for each genre. The 'foreach'
        # keyword argument allows us to compute the statistics for each genre in
        # parallel (i.e. a fan-out).
        self.next(self.compute_statistics, foreach="genres")

    @step
    def compute_statistics(self):
        """
        Compute statistics for a single genre.

        """
        # The genre currently being processed is a class property called
        # 'input'.
        self.genre = self.input
        print("Computing statistics for %s" % self.genre)

        # Find all the movies that have this genre and build a dataframe with
        # just those movies and just the columns of interest.
        selector = self.dataframe["genres"].apply(lambda row: self.genre in row)
        self.dataframe = self.dataframe[selector]
        self.dataframe = self.dataframe[["movie_title", "genres", "gross"]]

        # Get some statistics on the gross box office for these titles.
        points = [0.25, 0.5, 0.75]
        self.quartiles = self.dataframe["gross"].quantile(points).values

        # Join the results from other genres.
        self.next(self.join)

    @step
    def join(self, inputs):
        """
        Join our parallel branches and merge results into a dictionary.

        """
        # Merge results from the genre-specific computations.
        self.genre_stats = {
            inp.genre.lower(): {"quartiles": inp.quartiles, "dataframe": inp.dataframe}
            for inp in inputs
        }

        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.

        """
        pass


if __name__ == "__main__":
    MovieStatsFlow()
