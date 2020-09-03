# Episode 02-statistics: Is this Data Science?

**Use metaflow to load the movie metadata CSV file into a Pandas Dataframe and
compute some movie genre specific statistics. These statistics are then used in
later examples to improve our playlist generator. You can optionally use the
Metaflow client to eyeball the results in a Notebook, and make some simple
plots using the Matplotlib library.**

Please note that Episode 04, a follow-on to this episode, requires Pandas version 0.24.2.
Please make sure that you install or upgrade/downgrade to Pandas 0.24.2.

#### Showcasing:
- Fan-out over a set of parameters using Metaflow foreach.
- Using external packages like Pandas.
- Plotting results in a Notebook.

#### Before playing this episode:
1. ```python -m pip install pandas==0.24.2```
2. ```python -m pip install notebook```
3. ```python -m pip install matplotlib```

#### To play this episode:
1. ```cd metaflow-tutorials```
2. ```python 02-statistics/stats.py show```
3. ```python 02-statistics/stats.py run```
4. ```jupyter-notebook 02-statistics/stats.ipynb```
