"""
Unit tests for pandas DataFrame handling in card native-type conversion.

pandas >= 3.0 changed ``DataFrame.__module__`` from ``pandas.core.frame`` to
``pandas``, which shifted the type string that card serialization keys on and
broke ``Table.from_dataframe`` (it rendered "Object type pandas.DataFrame not
supported"). These tests pin the observable behavior so DataFrame detection
stays version-agnostic.
"""

import unittest

import pytest

pd = pytest.importorskip("pandas")

from metaflow.plugins.cards.card_modules.components import Table
from metaflow.plugins.cards.card_modules.convert_to_native_type import TaskToDict

_CANONICAL_DATAFRAME_TYPE = "pandas.core.frame.DataFrame"


def _obj_with_type(module, qualname):
    """Return an instance whose type reports ``module``/``qualname``.

    Lets us exercise a given pandas version's DataFrame class path (which
    ``object_type`` keys on) without installing that pandas version.
    """
    cls = type(qualname, (), {})
    cls.__module__ = module
    cls.__qualname__ = qualname
    return cls()


class TestPandasDataframeConversion(unittest.TestCase):
    """Card serialization must recognize pandas DataFrames on any pandas version."""

    def test_object_type_resolves_installed_dataframe(self):
        """A real DataFrame resolves to the canonical type name."""
        self.assertEqual(
            TaskToDict().object_type(pd.DataFrame([{"a": 1}])),
            _CANONICAL_DATAFRAME_TYPE,
        )

    def test_object_type_is_version_agnostic(self):
        """Both the pandas>=3.0 (``pandas.DataFrame``) and pandas<3.0
        (``pandas.core.frame.DataFrame``) class paths resolve to the canonical
        name, so detection is stable across pandas versions and older pandas
        keeps its exact behavior."""
        task_to_dict = TaskToDict()
        self.assertEqual(
            task_to_dict.object_type(_obj_with_type("pandas", "DataFrame")),
            _CANONICAL_DATAFRAME_TYPE,
        )
        self.assertEqual(
            task_to_dict.object_type(_obj_with_type("pandas.core.frame", "DataFrame")),
            _CANONICAL_DATAFRAME_TYPE,
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
