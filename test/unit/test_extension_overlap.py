"""
Tests for graceful handling of overlapping submodule promotions in
metaflow_extensions.

These tests validate the fix for:
  https://github.com/Netflix/metaflow/issues/3011

The ``alias_submodules`` function in ``metaflow.extension_support`` now:
  - Detects when two different extension packages promote the same alias
    via ``__mf_promote_submodules__``.
  - Emits a ``UserWarning`` identifying the conflicting packages and alias.
  - Applies a deterministic resolution: the *last-loaded* package wins.
  - Records every promotion in ``_promoted_aliases`` so callers (and tests)
    can inspect the final state.
"""

import types
import warnings

import pytest

from metaflow.extension_support import (
    EXT_PKG,
    alias_submodules,
    _promoted_aliases,
)


# ---------------------------------------------------------------------------
# Helpers – tiny fake modules that look like extension config modules
# ---------------------------------------------------------------------------

def _make_fake_module(name, promote_submodules=None, extra_attrs=None):
    """Return a ``types.ModuleType`` that mimics a Metaflow extension config
    module with an optional ``__mf_promote_submodules__`` list."""
    mod = types.ModuleType(name)
    mod.__package__ = name
    if promote_submodules is not None:
        mod.__mf_promote_submodules__ = promote_submodules
    if extra_attrs:
        for k, v in extra_attrs.items():
            setattr(mod, k, v)
    return mod


@pytest.fixture(autouse=True)
def _reset_promoted_aliases():
    """Ensure each test starts with a clean ``_promoted_aliases`` state."""
    saved = dict(_promoted_aliases)
    _promoted_aliases.clear()
    yield
    _promoted_aliases.clear()
    _promoted_aliases.update(saved)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSinglePackagePromotion:
    """No overlap – a single package promoting submodules should work without
    warnings and correctly register aliases."""

    def test_single_promotion_with_extension_point(self):
        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aliases = alias_submodules(mod, "org_a", "plugins")

        assert len(w) == 0, "No warnings expected for a single promotion"
        assert "metaflow.plugins.datatools" in aliases
        assert aliases["metaflow.plugins.datatools"] == (
            "%s.org_a.plugins.datatools" % EXT_PKG
        )
        assert "metaflow.plugins.datatools" in _promoted_aliases
        assert _promoted_aliases["metaflow.plugins.datatools"] == (
            "org_a",
            "%s.org_a.plugins.datatools" % EXT_PKG,
        )

    def test_single_promotion_toplevel(self):
        mod = _make_fake_module(
            "%s.org_a" % EXT_PKG,
            promote_submodules=["myutil"],
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aliases = alias_submodules(mod, "org_a", None)

        assert len(w) == 0
        assert "metaflow.myutil" in aliases
        assert aliases["metaflow.myutil"] == "%s.org_a.myutil" % EXT_PKG

    def test_multiple_submodules_no_overlap(self):
        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["foo", "bar", "baz"],
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aliases = alias_submodules(mod, "org_a", "plugins")

        assert len(w) == 0
        assert len(aliases) == 3
        for name in ("foo", "bar", "baz"):
            assert "metaflow.plugins.%s" % name in aliases


class TestOverlappingPromotions:
    """Two different packages promoting the same alias should emit a warning
    and the last-loaded package should win."""

    def test_overlap_emits_warning(self):
        """When org_a and org_b both promote 'datatools' under 'plugins', a
        UserWarning must be raised on the second call."""
        mod_a = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        mod_b = _make_fake_module(
            "%s.org_b.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )

        # First promotion – no warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            alias_submodules(mod_a, "org_a", "plugins")
        assert len(w) == 0

        # Second promotion – should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aliases_b = alias_submodules(mod_b, "org_b", "plugins")

        assert len(w) == 1
        warning_msg = str(w[0].message)
        assert "Overlapping submodule promotion" in warning_msg
        assert "org_a" in warning_msg
        assert "org_b" in warning_msg
        assert "metaflow.plugins.datatools" in warning_msg

    def test_overlap_last_loaded_wins(self):
        """The last-loaded package's target must be the final resolved value."""
        mod_a = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        mod_b = _make_fake_module(
            "%s.org_b.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            alias_submodules(mod_a, "org_a", "plugins")
            aliases_b = alias_submodules(mod_b, "org_b", "plugins")

        # org_b should win
        expected_target = "%s.org_b.plugins.datatools" % EXT_PKG
        assert aliases_b["metaflow.plugins.datatools"] == expected_target
        assert _promoted_aliases["metaflow.plugins.datatools"] == (
            "org_b",
            expected_target,
        )

    def test_overlap_toplevel(self):
        """Overlapping top-level (extension_point=None) promotions also warn."""
        mod_a = _make_fake_module(
            "%s.org_a" % EXT_PKG,
            promote_submodules=["myutil"],
        )
        mod_b = _make_fake_module(
            "%s.org_b" % EXT_PKG,
            promote_submodules=["myutil"],
        )

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            alias_submodules(mod_a, "org_a", None)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            alias_submodules(mod_b, "org_b", None)

        assert len(w) == 1
        assert "metaflow.myutil" in str(w[0].message)

    def test_multiple_overlapping_aliases(self):
        """If both packages promote several aliases and some overlap, only the
        overlapping ones trigger warnings."""
        mod_a = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["common", "unique_a"],
        )
        mod_b = _make_fake_module(
            "%s.org_b.plugins" % EXT_PKG,
            promote_submodules=["common", "unique_b"],
        )

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            alias_submodules(mod_a, "org_a", "plugins")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            alias_submodules(mod_b, "org_b", "plugins")

        # Only 'common' overlaps
        assert len(w) == 1
        assert "common" in str(w[0].message)

    def test_three_way_overlap(self):
        """Three packages promoting the same alias: two warnings total."""
        mods = [
            _make_fake_module(
                "%s.org_%s.plugins" % (EXT_PKG, name),
                promote_submodules=["shared"],
            )
            for name in ("a", "b", "c")
        ]
        all_warnings = []
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            for mod, name in zip(mods, ("org_a", "org_b", "org_c")):
                alias_submodules(mod, name, "plugins")
            all_warnings = list(w)

        assert len(all_warnings) == 2
        # The last package should win
        assert _promoted_aliases["metaflow.plugins.shared"][0] == "org_c"


class TestSamePackageRepromotion:
    """If the *same* package re-promotes the same alias (e.g. loaded twice),
    no warning should be raised because it's not a conflict."""

    def test_same_package_no_warning(self):
        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            alias_submodules(mod, "org_a", "plugins")
            alias_submodules(mod, "org_a", "plugins")

        assert len(w) == 0, (
            "Re-promoting the same alias from the same package should not warn"
        )


class TestNoPromotionSubmodules:
    """Modules without ``__mf_promote_submodules__`` should not touch
    ``_promoted_aliases`` at all."""

    def test_no_promote_attribute(self):
        mod = _make_fake_module("%s.org_a.plugins" % EXT_PKG)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aliases = alias_submodules(mod, "org_a", "plugins")

        assert len(w) == 0
        assert len(aliases) == 0
        assert len(_promoted_aliases) == 0

    def test_empty_promote_list(self):
        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=[],
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aliases = alias_submodules(mod, "org_a", "plugins")

        assert len(w) == 0
        assert len(aliases) == 0


class TestWarningContent:
    """Verify that the warning message contains all the information an
    extension developer needs to debug the conflict."""

    def test_warning_includes_all_details(self):
        mod_a = _make_fake_module(
            "%s.alpha_corp.plugins" % EXT_PKG,
            promote_submodules=["s3tools"],
        )
        mod_b = _make_fake_module(
            "%s.beta_corp.plugins" % EXT_PKG,
            promote_submodules=["s3tools"],
        )

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            alias_submodules(mod_a, "alpha_corp", "plugins")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            alias_submodules(mod_b, "beta_corp", "plugins")

        msg = str(w[0].message)
        # All relevant pieces present
        assert "metaflow.plugins.s3tools" in msg
        assert "beta_corp" in msg  # the overriding package
        assert "alpha_corp" in msg  # the overridden package
        assert "%s.beta_corp.plugins.s3tools" % EXT_PKG in msg
        assert "%s.alpha_corp.plugins.s3tools" % EXT_PKG in msg
        assert "__mf_promote_submodules__" in msg


class TestGetPromotedAliases:
    """Test the public ``get_promoted_aliases()`` accessor."""

    def test_returns_copy(self):
        from metaflow.extension_support import get_promoted_aliases

        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        alias_submodules(mod, "org_a", "plugins")

        result = get_promoted_aliases()
        assert isinstance(result, dict)
        assert "metaflow.plugins.datatools" in result

        # Mutating the returned dict must NOT affect the internal state
        result["metaflow.plugins.datatools"] = ("hacked", "hacked")
        assert _promoted_aliases["metaflow.plugins.datatools"] != ("hacked", "hacked")


class TestExtraIndentFormatting:
    """Ensure the extra_indent parameter still works correctly after the
    refactor."""

    def test_extra_indent_true(self):
        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        # Should not raise; just exercises the indented debug path
        aliases = alias_submodules(mod, "org_a", "plugins", extra_indent=True)
        assert "metaflow.plugins.datatools" in aliases

    def test_extra_indent_false(self):
        mod = _make_fake_module(
            "%s.org_a.plugins" % EXT_PKG,
            promote_submodules=["datatools"],
        )
        aliases = alias_submodules(mod, "org_a", "plugins", extra_indent=False)
        assert "metaflow.plugins.datatools" in aliases
