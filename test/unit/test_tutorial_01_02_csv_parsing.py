import csv
import pytest

# Module-level constant for immutable test data
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

    # Note: If testing quoted newlines, replace data.splitlines() with io.StringIO(data)
    for row in csv.DictReader(data.splitlines()):
        for c in cols:
            val = int(row[c]) if c in int_cols else row[c]
            result[c].append(val)
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "columns, expected_data",
    [
        (
            ["movie_title", "genres"],
            {
                "movie_title": ["Monsters, Inc.", "I, Robot"],
                "genres": ["Animation|Comedy", "Action|Sci-Fi"],
            },
        ),
        (
            ["movie_title", "title_year", "genres", "gross"],
            {
                "movie_title": ["Monsters, Inc.", "I, Robot"],
                "title_year": [2001, 2004],
                "genres": ["Animation|Comedy", "Action|Sci-Fi"],
                "gross": [289907418, 144795350],
            },
        ),
    ],
    ids=["subset_of_columns", "all_columns"],
)
def test_parse_csv_extracts_requested_columns(columns, expected_data):
    """
    Test that parse_csv correctly extracts, filters, and types the specified columns.
    """
    # Act
    df = parse_csv(SAMPLE_CSV, columns)

    # Assert: Dataframe keeps exactly the requested number of columns
    assert len(df) == len(columns)

    # Assert: All values are correctly aligned, typed, and no rows are dropped
    for col in columns:
        assert len(df[col]) == 2
        assert df[col] == expected_data[col]
