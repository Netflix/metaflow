from pathlib import Path
import pytest
from metaflow.runner.metaflow_runner import Runner

ROOT_PATH = Path(__file__).resolve().parents[2]
PLAYLIST_FLOW_PATH = ROOT_PATH / "metaflow/tutorials/01-playlist/playlist.py"
STATS_FLOW_PATH = ROOT_PATH / "metaflow/tutorials/02-statistics/stats.py"
CSV_SAMPLE_PATH = Path(__file__).parent / "test_tutorial_01_02_csv_sample.csv"

@pytest.fixture(scope="module")
def PlaylistFlow():
    """
    Run PlayListFlow and return the Run object

    """
    with Runner(str(PLAYLIST_FLOW_PATH), show_output=False) as r: 
        return r.run(movie_data=str(CSV_SAMPLE_PATH)).run

@pytest.fixture(scope="module")
def StatsFlow():
    """
    Run MovieStatsFlow and return the Run object
    
    """
    with Runner(str(STATS_FLOW_PATH), show_output=False) as r:
        return r.run(movie_data=str(CSV_SAMPLE_PATH)).run

def test_playlist_flow_success(PlaylistFlow):
    """
    Check if the flow executed without any error
    
    """
    assert PlaylistFlow.finished
    assert PlaylistFlow.successful


def test_playlist_csv_parsing(PlaylistFlow):
    """
    1) Title with commas is parsed as a single field, not split
    2) All values are correctly aligned to their respective columns
    3) No rows are dropped or duplicated
    4) Dataframe keeps exactly 2 columns: movie_title and genres

    """
    df = PlaylistFlow["start"].task.data.dataframe
    assert {"Monsters, Inc.", "I, Robot"} <= set(df["movie_title"])
    assert {"Animation|Comedy", "Action|Sci-Fi"} <= set(df["genres"])
    assert all(len(col) == 2 for col in df.values())
    assert len(df) == 2

def test_stats_flow_success(StatsFlow):
    """
    Check if the flow executed without any error
    
    """
    assert StatsFlow.finished
    assert StatsFlow.successful

def test_stats_csv_parsing(StatsFlow):
    """
    1) Title with commas is parsed as a single field, not split
    2) All values are correctly aligned to their respective columns
    3) No rows are dropped or duplicated across all columns
    4) Dataframe keeps exactly 4 columns: movie_title, title_year, genres, gross

    """
    df = StatsFlow["start"].task.data.dataframe
    assert {"Monsters, Inc.", "I, Robot"} <= set(df["movie_title"])
    assert {2001, 2004} <= set(df["title_year"])
    assert {"Animation|Comedy", "Action|Sci-Fi"} <= set(df["genres"])
    assert {289907418, 144795350} <= set(df["gross"])
    assert all(len(col) == 2 for col in df.values())
    assert len(df) == 4
