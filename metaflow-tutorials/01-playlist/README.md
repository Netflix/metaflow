# Episode 01-playlist: Let's build you a movie playlist.

**This flow loads a movie metadata CSV file and builds a playlist for your
favorite movie genre. Everything in Metaflow is versioned, so you can run it
multiple times and view all the historical playlists with the Metaflow client
in a Notebook.**

#### Showcasing:
- Including external files with 'IncludeFile'.
- Basic Metaflow Parameters.
- Running workflow branches in parallel and joining results.
- Using the Metaflow client in a Notebook.

#### Before playing this episode:
1. ```python -m pip install notebook```

#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python 01-playlist/playlist.py show```
3. ```python 01-playlist/playlist.py run```
4. ```python 01-playlist/playlist.py run --genre comedy```
5. ```jupyter-notebook 01-playlist/playlist.ipynb```
