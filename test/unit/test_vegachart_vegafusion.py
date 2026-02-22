"""
Test VegaChart.from_altair_chart with vegafusion data transformer.

Reproduces the bug described in issue #2471 where VegaChart.from_altair_chart
raises ValueError when the vegafusion data transformer is enabled.
"""

import json
import sys

import pytest


# These tests require altair and vegafusion to be installed.
alt = pytest.importorskip("altair")
pd = pytest.importorskip("pandas")
pytest.importorskip("vegafusion")

try:
    from metaflow.plugins.cards.card_modules.components import VegaChart
except Exception:
    # On Windows, metaflow's __init__.py triggers plugin imports that
    # require fcntl (Linux-only). Fall back to loading the card_modules
    # package with proper parent packages so relative imports work.
    import importlib
    import importlib.util
    import os
    import types

    _base = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "metaflow",
            "plugins",
            "cards",
            "card_modules",
        )
    )

    # Register stub parent packages so relative imports resolve
    for pkg in [
        "metaflow",
        "metaflow.plugins",
        "metaflow.plugins.cards",
        "metaflow.plugins.cards.card_modules",
    ]:
        if pkg not in sys.modules:
            mod = types.ModuleType(pkg)
            mod.__path__ = [
                os.path.normpath(
                    os.path.join(
                        os.path.dirname(__file__),
                        "..",
                        "..",
                        *pkg.split("."),
                    )
                )
            ]
            mod.__package__ = pkg
            sys.modules[pkg] = mod

    def _load(name, filepath):
        full_name = f"metaflow.plugins.cards.card_modules.{name}"
        spec = importlib.util.spec_from_file_location(
            full_name,
            filepath,
            submodule_search_locations=[],
        )
        mod = importlib.util.module_from_spec(spec)
        mod.__package__ = "metaflow.plugins.cards.card_modules"
        sys.modules[full_name] = mod
        spec.loader.exec_module(mod)
        return mod

    _load("card", os.path.join(_base, "card.py"))
    _load("convert_to_native_type", os.path.join(_base, "convert_to_native_type.py"))
    _load("renderer_tools", os.path.join(_base, "renderer_tools.py"))
    _load("basic", os.path.join(_base, "basic.py"))
    _load("json_viewer", os.path.join(_base, "json_viewer.py"))
    components = _load("components", os.path.join(_base, "components.py"))
    VegaChart = components.VegaChart


@pytest.fixture(autouse=True)
def reset_transformer():
    """Reset altair transformer to default after each test."""
    yield
    alt.data_transformers.enable("default")


class TestVegaChartFromAltairChart:
    """Tests for VegaChart.from_altair_chart with various data transformers."""

    def _make_chart(self):
        """Create a simple Altair bar chart for testing."""
        df = pd.DataFrame(
            {"item": ["apple", "avocado", "fish"], "cost": [2, 5, 9]}
        )
        return alt.Chart(df).mark_bar().encode(
            x="item", y="cost", tooltip=["item", "cost"]
        )

    def test_default_transformer(self):
        """VegaChart.from_altair_chart works with default transformer."""
        alt.data_transformers.enable("default")
        chart = self._make_chart()

        vc = VegaChart.from_altair_chart(chart)

        assert isinstance(vc._spec, dict)
        assert "$schema" in vc._spec
        assert "vega-lite" in vc._spec["$schema"]

    def test_vegafusion_transformer(self):
        """VegaChart.from_altair_chart works when vegafusion is enabled.

        Reproduces the bug from issue #2471: calling to_dict() without
        format='vega' raises ValueError when vegafusion is active.
        The fix catches this and retries with format='vega'.
        """
        alt.data_transformers.enable("vegafusion")
        chart = self._make_chart()

        # Before the fix, this raised:
        # ValueError: When the "vegafusion" data transformer is enabled,
        # the to_dict() and to_json() chart methods must be called
        # with format="vega".
        vc = VegaChart.from_altair_chart(chart)

        assert isinstance(vc._spec, dict)
        assert "$schema" in vc._spec
        # vegafusion produces Vega (not Vega-Lite) output
        assert "vega/v" in vc._spec["$schema"]
        # Spec must be JSON-serializable for card rendering
        assert len(json.dumps(vc._spec)) > 50

    def test_vegafusion_large_dataset(self):
        """VegaChart handles large datasets (>5000 rows) with vegafusion.

        Users enable vegafusion specifically to bypass Altair's 5000-row
        limit, so this is the primary real-world use case.
        """
        import numpy as np

        alt.data_transformers.enable("vegafusion")

        np.random.seed(42)
        n = 6000
        df = pd.DataFrame(
            {
                "x": np.random.randn(n),
                "y": np.random.randn(n),
                "category": np.random.choice(["A", "B", "C"], n),
            }
        )
        chart = alt.Chart(df).mark_circle(size=10).encode(
            x="x:Q", y="y:Q", color="category:N"
        )

        vc = VegaChart.from_altair_chart(chart)

        assert isinstance(vc._spec, dict)
        json_str = json.dumps(vc._spec)
        assert len(json_str) > 1000

    def test_vegafusion_layered_chart(self):
        """VegaChart handles layered charts with vegafusion enabled."""
        alt.data_transformers.enable("vegafusion")

        df = pd.DataFrame({"x": range(50), "y": [i**2 for i in range(50)]})
        line = alt.Chart(df).mark_line().encode(x="x:Q", y="y:Q")
        points = alt.Chart(df).mark_point().encode(x="x:Q", y="y:Q")

        vc = VegaChart.from_altair_chart(line + points)

        assert isinstance(vc._spec, dict)
        assert "$schema" in vc._spec

    def test_render_with_vegafusion(self):
        """render() produces valid card component dict after vegafusion."""
        alt.data_transformers.enable("vegafusion")
        chart = self._make_chart()

        vc = VegaChart.from_altair_chart(chart)
        rendered = vc.render()

        assert isinstance(rendered, dict)
        assert rendered["type"] == "vegaChart"
        assert "spec" in rendered
        assert "id" in rendered

    def test_rejects_non_altair_object(self):
        """from_altair_chart raises ValueError for non-Altair objects."""
        with pytest.raises(ValueError, match="is not an altair chart"):
            VegaChart.from_altair_chart({"not": "a chart"})
