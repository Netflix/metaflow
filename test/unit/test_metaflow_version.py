import pytest

from metaflow.metaflow_version import format_git_describe


@pytest.mark.parametrize(
    "git_str,public,expected",
    [
        # Plain tag at the tagged commit.
        ("2.19.22-0-g4ff334e", False, "2.19.22"),
        ("2.19.22-0-g4ff334e", True, "2.19.22"),
        # Plain tag, N commits ahead.
        ("2.19.22-5-g4ff334e", False, "2.19.22.post5-git4ff334e"),
        ("2.19.22-5-g4ff334e", True, "2.19.22.post5"),
        # Plain tag, dirty worktree.
        ("2.19.22-5-g4ff334e-dirty", False, "2.19.22.post5-git4ff334e-dirty"),
        ("2.19.22-0-g4ff334e-dirty", False, "2.19.22-dirty"),
        # Dashed (PEP 440 pre-release) tag at the tagged commit.
        ("v1.0-rc.1-0-gabcdef0", False, "v1.0-rc.1"),
        # Dashed tag, N commits ahead.
        ("v1.0-rc.1-12-gabcdef0", False, "v1.0-rc.1.post12-gitabcdef0"),
        ("v1.0-rc.1-12-gabcdef0", True, "v1.0-rc.1.post12"),
        # Dashed tag, dirty worktree — this is what used to crash with
        # ValueError: too many values to unpack (expected 3).
        ("v1.0-rc.1-12-gabcdef0-dirty", False, "v1.0-rc.1.post12-gitabcdef0-dirty"),
        # Tag with multiple internal dashes.
        (
            "v9.2.97-rc.15-100-g3a13f86-dirty",
            False,
            "v9.2.97-rc.15.post100-git3a13f86-dirty",
        ),
    ],
)
def test_format_git_describe_parses_known_shapes(git_str, public, expected):
    assert format_git_describe(git_str, public=public) == expected


@pytest.mark.parametrize(
    "git_str",
    [
        None,
        # Fewer than three dash-separated tokens — caller falls back to
        # __version__ when format_git_describe returns None.
        "short",
        "a-b",
        # Three tokens with trailing "dirty" — not a real describe output
        # but guarded symmetrically with the clean branch.
        "a-b-dirty",
    ],
)
def test_format_git_describe_returns_none_for_unparseable(git_str):
    assert format_git_describe(git_str) is None
