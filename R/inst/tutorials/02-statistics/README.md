# Episode 02-statistics: Is this Data Science?

**Use metaflow to load the movie metadata CSV file into a data frame and compute some movie genre-specific statistics. These statistics are then used in
later examples to improve our playlist generator. You can optionally use the
Metaflow client to eyeball the results in a Markdown Notebook, and make some simple
plots.**

#### Showcasing:
- Fan-out over a set of parameters using Metaflow foreach.
- Plotting results in a Markdown Notebook.

#### Before playing this episode:
1. Configure your metadata provider to a user-wise global provider, if you haven't done it already. 
```bash
$mkdir -p /path/to/home/.metaflow
$export METAFLOW_DEFAULT_METADATA=local
```

#### To play this episode:
##### Execute the flow:
In a terminal:
1. ```cd tutorials/02-statistics```
2. ```Rscript stats.R show```
3. ```Rscript stats.R run```

If you are using RStudio, you can run this script by directly executing `source("stats.R")`.

##### Inspect the results:
Open the R Markdown file ```stats.Rmd``` in RStudio and execute the markdown cells.