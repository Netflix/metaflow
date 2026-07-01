"""
Unit tests for pandas DataFrame handling in card native-type conversion.

pandas >= 3.0 changed ``DataFrame.__module__`` from ``pandas.core.frame`` to
``pandas``, which shifted the type string that card serialization keys on and
broke ``Table.from_dataframe`` (it rendered "Object type pandas.DataFrame not
supported"). These tests pin the normalization so DataFrame detection stays
version-agnostic.
"""

import unittest

import pytest

pd = pytest.importorskip("pandas")

from metaflow.plugins.cards.card_modules.components import Table
from metaflow.plugins.cards.card_modules.convert_to_native_type import (
    TaskToDict,
    _normalize_type_name,
)

_CANONICAL_DATAFRAME_TYPE = "pandas.core.frame.DataFrame"


class TestPandasDataframeConversion(unittest.TestCase):
    """Card serialization must recognize pandas DataFrames on any pandas version."""

    def test_object_type_is_canonical_across_pandas_versions(self):
        """object_type resolves a DataFrame to the canonical name regardless of
        the (version-dependent) module path of the class."""
        df = pd.DataFrame([{"a": 1}])
        self.assertEqual(TaskToDict().object_type(df), _CANONICAL_DATAFRAME_TYPE)

    def test_normalize_type_name_maps_moved_name(self):
        """The pandas>=3.0 name is normalized; unrelated names pass through."""
        self.assertEqual(
            _normalize_type_name("pandas.DataFrame"), _CANONICAL_DATAFRAME_TYPE
        )
        self.assertEqual(_normalize_type_name("builtins.int"), "builtins.int")

    def test_normalize_type_name_is_noop_for_pandas2(self):
        """Backwards compatible: the canonical name pandas<3.0 already produces
        is passed through unchanged, so older pandas keeps its exact behavior."""
        self.assertEqual(
            _normalize_type_name(_CANONICAL_DATAFRAME_TYPE), _CANONICAL_DATAFRAME_TYPE
        )

    def test_table_from_dataframe_renders_rows(self):
        """Table.from_dataframe renders the data instead of an
        "Object type ... not supported" placeholder."""
        df = pd.DataFrame(
            [{"metric": "MAE", "value": 1.5}, {"metric": "RMSE", "value": 2.0}]
        )
        rendered = Table.from_dataframe(df).render()

        self.assertEqual(rendered["type"], "table")
        self.assertIn("metric", rendered["columns"])
        self.assertIn("value", rendered["columns"])
        # An unrecognized type would render a single "Object type ... not
        # supported" column instead of the data.
        self.assertFalse(
            any("not supported" in str(col) for col in rendered["columns"])
        )
        self.assertEqual(len(rendered["data"]), len(df))
