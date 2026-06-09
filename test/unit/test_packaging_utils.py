import os
import pytest

from metaflow.packaging_sys.utils import walk


@pytest.mark.parametrize(
    "root_parts, files_to_create, expected_results",
    [
        (
            # Root is under a hidden ancestor.
            # Regression: hidden ancestor dirs must not exclude user files.
            [".hidden_parent", "project", "flows"],
            ["hello_flow.py"],
            ["hello_flow.py"],
        ),
        (
            # Root contains both visible files and hidden directories.
            # Hidden directories *under* root should be excluded.
            [".hidden_parent", "project"],
            ["visible.py", ".secret/hidden.py"],
            ["visible.py"],
        ),
    ],
    ids=[
        "hidden_ancestor_allows_visible_children",
        "hidden_descendant_is_excluded",
    ],
)
def test_walk_handles_hidden_directories(
    tmp_path, root_parts, files_to_create, expected_results
):
    # Setup: Create dynamic root structure
    root_dir = tmp_path.joinpath(*root_parts)
    root_dir.mkdir(parents=True, exist_ok=True)

    # Setup: Create files within the root
    for file_path in files_to_create:
        normalized_file = os.path.normpath(file_path)
        full_path = root_dir / normalized_file
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text("")

    # Act
    # walk yields (absolute_path, relative_path) tuples
    results = {rel for _, rel in walk(str(root_dir), exclude_hidden=True)}

    expected_set = {os.path.normpath(p) for p in expected_results}

    # Assert: Exact set matching guarantees both inclusion AND exclusion
    assert results == expected_set, f"Expected {expected_set}, got {results}"
