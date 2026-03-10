from metaflow import FlowSpec, step, Parameter
import csv

class MovieStatsFlow(FlowSpec):
    movies_file = Parameter('movies', default='movies.csv', help="The CSV file to process")

    @step
    def start(self):
        # Read data from the CSV file
        with open(self.movies_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.data = [row for row in reader]
        
        # Get unique genres for the foreach
        self.genres = list(set(row['genre'] for row in self.data))
        self.next(self.compute_stats, foreach='genres')

    @step
    def compute_stats(self):
        self.genre = self.input
        # Filter data for the specific genre
        genre_data = [row for row in self.data if row['genre'] == self.genre]
        
        # Calculate real statistics (Quartiles of the movie scores)
        # Assuming the CSV has a 'score' or 'rating' column
        scores = sorted([int(row['score']) for row in genre_data if row['score'].isdigit()])
        
        if scores:
            n = len(scores)
            self.quartiles = [
                scores[n // 4],    # Q1
                scores[n // 2],    # Median
                scores[3 * n // 4] # Q3
            ]
        else:
            self.quartiles = [0, 0, 0]
            
        self.count = len(genre_data)
        self.next(self.join)

    @step
    def join(self, inputs):
        # Merge results into a single dictionary
        self.stats = {
            inp.genre: {'count': inp.count, 'quartiles': inp.quartiles}
            for inp in inputs
        }
        self.next(self.end)

    @step
    def end(self):
        print("Statistics computation complete.")

if __name__ == '__main__':
    MovieStatsFlow()