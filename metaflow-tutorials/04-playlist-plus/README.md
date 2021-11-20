# Episode 04-playlist-plus: The Final Showdown.

**Now that we've improved our genre based playlist generator. We expose a 'hint'
parameter allowing the user to suggest a better bonus movie. The bonus movie is
chosen from the movie that has the most similar name to the 'hint'.
This is achieved by importing a string edit distance package using Metaflow's
conda based dependency management feature. Dependency management builds
isolated and reproducible environments for individual steps.**

#### Showcasing:
- Metaflow's conda based dependency management.

#### Before playing this episode:
1. This tutorial requires the 'conda' package manager to be installed with the
   conda-forge channel added.
   a. Download Miniconda at https://docs.conda.io/en/latest/miniconda.html
   b. ```conda config --add channels conda-forge```

#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python 04-playlist-plus/playlist.py --environment=conda show```
3. ```python 04-playlist-plus/playlist.py --environment=conda run```
4. ```python 04-playlist-plus/playlist.py --environment=conda run --hint "Data Science Strikes Back"```
