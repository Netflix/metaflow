import csv
from io import StringIO

SAMPLE_CSV = (
    "movie_title,title_year,genres,gross\n"
    '"Monsters, Inc.",2001,Animation|Comedy,289907418\n'
    '"I, Robot",2004,Action|Sci-Fi,144795350\n'
)


def parse_csv(data, cols):
    """
    Parse CSV into dataframe

    """
    result = {c: [] for c in cols}
    int_cols = ("title_year", "gross")
    for row in csv.DictReader(data.splitlines()):
        for c in cols:
            val = int(row[c]) if c in int_cols else row[c]
            result[c].append(val)
    return result


def test_playlist_csv_parsing():
    """
    Validate Test Cases For Tutorial 01

    """
    df = parse_csv(SAMPLE_CSV, ["movie_title", "genres"])

    # Title with commas is parsed as a single field
    assert {"Monsters, Inc.", "I, Robot"} <= set(df["movie_title"])

    # All values are correctly aligned to their respective columns
    assert {"Animation|Comedy", "Action|Sci-Fi"} <= set(df["genres"])

    # No rows are dropped or duplicated
    assert all(len(col) == 2 for col in df.values())

    # Dataframe keeps exactly 2 columns: movie_title and genres
    assert len(df) == 2


def test_stats_csv_parsing():
    """
    Validate Test Cases For Tutorial 02

    """
    df = parse_csv(SAMPLE_CSV, ["movie_title", "title_year", "genres", "gross"])

    # Title with commas is parsed as a single field
    assert {"Monsters, Inc.", "I, Robot"} <= set(df["movie_title"])

    # All values are correctly aligned to their respective columns
    assert {2001, 2004} <= set(df["title_year"])
    assert {"Animation|Comedy", "Action|Sci-Fi"} <= set(df["genres"])
    assert {289907418, 144795350} <= set(df["gross"])

    # No rows are dropped or duplicated
    assert all(len(col) == 2 for col in df.values())

    # Dataframe keeps exactly 4 columns: movie_title, title_year, genres, gross
    assert len(df) == 4
