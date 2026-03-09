from metaflow import FlowSpec, step, Parameter

class StatisticsReduxFlow(FlowSpec):
    # This parameter allows us to pass different movie data files
    movie_data = Parameter('movies', default='movies.csv')

    @step
    def start(self):
        # We simulate loading the data here
        print(f"Loading data from {self.movie_data}")
        self.genres = ['Sci-Fi', 'Comedy', 'Drama']
        self.next(self.compute_stats, foreach='genres')

    @step
    def compute_stats(self):
        # This runs in parallel for each genre
        self.genre = self.input
        print(f"Computing stats for {self.genre}")
        self.count = 42 # Mocking the computation
        self.next(self.join)

    @step
    def join(self, inputs):
        # Merging the parallel results back together
        self.results = {inp.genre: inp.count for inp in inputs}
        self.next(self.end)

    @step
    def end(self):
        print("Final Results:", self.results)
        print("StatisticsRedux is finished.")

if __name__ == "__main__":
    StatisticsReduxFlow()