import os
from metaflow import FlowSpec, step, IncludeFile
import csv

def script_path(filename):
    return os.path.join(os.path.dirname(__file__), filename)

class MovieStatsFlow(FlowSpec):
    movie_data = IncludeFile(
        'movie_data',
        help='The path to a movie metadata file.',
        default=script_path('../02-statistics/movies.csv'),
    )

    @step
    def start(self):
        lines = [line for line in self.movie_data.split('\n') if line]
        reader = csv.DictReader(lines)
        self.data = [row for row in reader]
        
        self.genres = list({
            genre for row in self.data
            for genre in row['genres'].split('|')
        })
        self.next(self.compute_stats, foreach='genres')

    @step
    def compute_stats(self):
        self.genre = self.input
        genre_data = [
            row for row in self.data 
            if self.genre in row['genres'].split('|')
        ]
        
        scores = sorted([int(row['gross']) for row in genre_data if row['gross'].isdigit()])
        
        if scores:
            n = len(scores)
            self.quartiles = [
                scores[n // 4],
                scores[n // 2],
                scores[3 * n // 4]
            ]
        else:
            self.quartiles = [0, 0, 0]
            
        self.count = len(genre_data)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.genre_stats = {
            inp.genre: {'count': inp.count, 'quartiles': inp.quartiles}
            for inp in inputs
        }
        self.next(self.end)

    @step
    def end(self):
        print("Statistics computation complete.")

if __name__ == '__main__':
    MovieStatsFlow()