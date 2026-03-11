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
        
        # Consistent with Tutorial 02 logic
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
        
        # Matches Tutorial 02: Direct cast (fails if data is bad, which mentors prefer for tutorials)
        scores = sorted([int(row['gross']) for row in genre_data])
        
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
        # FIX: Added .lower() to the key to match Tutorial 02 consistency
        self.genre_stats = {
            inp.genre.lower(): {'count': inp.count, 'quartiles': inp.quartiles}
            for inp in inputs
        }
        self.next(self.end)

    @step
    def end(self):
        print("Statistics computation complete.")

if __name__ == '__main__':
    MovieStatsFlow()