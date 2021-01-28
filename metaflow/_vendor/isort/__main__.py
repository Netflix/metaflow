from __future__ import absolute_import

from metaflow._vendor.isort.pie_slice import apply_changes_to_python_environment

apply_changes_to_python_environment()

from metaflow._vendor.isort.main import main  # noqa: E402 isort:skip

main()
