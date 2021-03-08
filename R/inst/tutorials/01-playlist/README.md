# Episode 01-playlist: Let's build you a movie playlist.

**This flow loads a movie metadata CSV file and builds a playlist for your
favorite movie genre. Everything in Metaflow is versioned, so you can run it
multiple times and view all the historical playlists with the Metaflow client
in an R Markdown Notebook.**

#### Showcasing:
- Basic Metaflow Parameters.
- Running workflow branches in parallel and joining results.
- Using the Metaflow client in an R Markdown Notebook.

#### To play this episode:
##### Execute the flow:
Inside a terminal:
1. ```cd tutorials/01-playlist/```
2. ```Rscript playlist.R show```
3. ```Rscript playlist.R run```
4. ```Rscript playlist.R run --genre comedy```

If you are using RStudio, you can replace the `run()` in last line in `playlist.R` with `run(genre="comedy")`, and run the episode by executing `source("playlist.R")` in RStudio.

##### Inspect the results
Open the R Markdown file ```playlist.Rmd``` in RStudio and execute the markdown cells.