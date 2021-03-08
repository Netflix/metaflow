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
In a terminal:
1. ```cd tutorials/03-playlist-redux```
2. ```Rscript playlist.R show```
3. ```Rscript playlist.R run```

If you are using RStudio, you can run this script by directly executing `source("playlist.R")`.

In this ```PlayListReduxFlow```, we reuse the genre median gross box office statistics computed from ```MoviesStatsFlow```, pick the genre with the highest median gross box office, and create a randomized playlist of movies of this picked genre.