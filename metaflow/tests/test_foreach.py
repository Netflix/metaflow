from os.path import dirname, join

from metaflow.flowspec import FlowSpec

from metaflow.tests.flows.stats_02 import NewMovieStatsFlow
from metaflow.tests.utils import (
    check_graph,
    parametrize,
    run,
    test_flows_dir,
    tutorials_dir,
)


expected_genres = [
    "action",
    "adventure",
    "animation",
    "biography",
    "comedy",
    "crime",
    "documentary",
    "drama",
    "family",
    "fantasy",
    "film-noir",
    "history",
    "horror",
    "music",
    "musical",
    "mystery",
    "romance",
    "sci-fi",
    "sport",
    "thriller",
    "war",
    "western",
]

root_dir = dirname(dirname(__file__))
flow_files = {
    "MovieStatsFlow": join(tutorials_dir, "02-statistics", "stats.py"),
    "NewMovieStatsFlow": join(test_flows_dir, "stats_02.py"),
}


@parametrize(
    "flow",
    [
        "MovieStatsFlow",
        "NewMovieStatsFlow",
        NewMovieStatsFlow,
    ],
)
def test_foreach(flow):
    if isinstance(flow, str):
        file = flow_files[flow]
        flow = FlowSpec.load("%s:%s" % (file, flow), register_main="overwrite")

    expected = [
        {
            "name": "start",
            "type": "foreach",
            "in_funcs": [],
            "out_funcs": ["compute_statistics"],
            "split_parents": [],
        },
        {
            "name": "compute_statistics",
            "type": "linear",
            "in_funcs": ["start"],
            "out_funcs": ["join"],
            "split_parents": ["start"],
        },
        {
            "name": "join",
            "type": "join",
            "in_funcs": ["compute_statistics"],
            "out_funcs": ["end"],
            "split_parents": ["start"],
        },
        {
            "name": "end",
            "type": "end",
            "in_funcs": ["join"],
            "out_funcs": [],
            "split_parents": [],
        },
    ]

    check_graph(flow, expected)
    data = run(flow)
    genre_stats = data["genre_stats"]
    assert sorted(list(genre_stats.keys())) == expected_genres
