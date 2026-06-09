import pytest

from metaflow.packaging_sys.utils import walk


@pytest.mark.parametrize(
    "root_parts, files_to_create, expected_included, expected_excluded",
    [
        (
            # Root is under a hidden ancestor.
            # Regression: hidden ancestor dirs must not exclude user files.
            [".hidden_parent", "project", "flows"],
            ["hello_flow.py"],
            ["hello_flow.py"],
            [],
        ),
        (
            # Root contains both visible files and hidden directories.
            # Hidden directories *under* root should be excluded.
            [".hidden_parent", "project"],
            ["visible.py", ".secret/hidden.py"],
            ["visible.py"],
            ["hidden.py", ".secret"],
        ),
    ],
    ids=[
        "hidden_ancestor_allows_visible_children",
        "hidden_descendant_is_excluded",
    ],
)
def test_walk_handles_hidden_directories(
    tmp_path, root_parts, files_to_create, expected_included, expected_excluded
):
    # Setup: Create dynamic root structure
    root_dir = tmp_path.joinpath(*root_parts)
    root_dir.mkdir(parents=True, exist_ok=True)

    # Setup: Create files within the root
    for file_path in files_to_create:
        full_path = root_dir / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text("")

    # Act
    # walk yields (absolute_path, relative_path) tuples
    results = {rel for _, rel in walk(str(root_dir), exclude_hidden=True)}

    # Assert
    for expected in expected_included:
        assert any(
            expected in r for r in results
        ), f"Expected {expected} in walk results, got: {results}"

    for excluded in expected_excluded:
        assert not any(
            excluded in r for r in results
        ), f"Expected {excluded} to be excluded, got: {results}"
