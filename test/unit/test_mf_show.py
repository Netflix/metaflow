"""Unit tests for metaflow.plugins.jupyter.mf_magics (%mf_show magic)."""

import unittest
from unittest.mock import MagicMock, patch

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class TestRender(unittest.TestCase):
    @unittest.skipUnless(HAS_PANDAS, "pandas not installed")
    def test_dataframe_calls_display(self):
        from metaflow.plugins.jupyter.mf_magics import _render

        df = pd.DataFrame({"x": [1, 2]})
        with patch("metaflow.plugins.jupyter.mf_magics.display") as mock_display:
            _render(df)
        mock_display.assert_called_once_with(df)

    def test_object_with_plot_calls_plot(self):
        from metaflow.plugins.jupyter.mf_magics import _render

        obj = MagicMock(spec=["plot"])
        with patch("metaflow.plugins.jupyter.mf_magics.display"):
            _render(obj)
        obj.plot.assert_called_once()

    def test_plain_object_falls_back_to_display(self):
        from metaflow.plugins.jupyter.mf_magics import _render

        obj = object()
        with patch("metaflow.plugins.jupyter.mf_magics.display") as mock_display:
            _render(obj)
        mock_display.assert_called_once_with(obj)


class TestMfShow(unittest.TestCase):
    def test_empty_line_prints_usage(self):
        from metaflow.plugins.jupyter.mf_magics import _mf_show

        with patch("metaflow.plugins.jupyter.mf_magics.IPython") as mock_ipy:
            mock_ipy.get_ipython.return_value = MagicMock()
            with patch("builtins.print") as mock_print:
                _mf_show("")
        self.assertIn("Usage", mock_print.call_args[0][0])

    def test_eval_error_prints_message(self):
        from metaflow.plugins.jupyter.mf_magics import _mf_show

        ip = MagicMock()
        ip.ev.side_effect = NameError("self not defined")
        with patch("metaflow.plugins.jupyter.mf_magics.IPython") as mock_ipy:
            mock_ipy.get_ipython.return_value = ip
            with patch("builtins.print") as mock_print:
                _mf_show("self.df")
        self.assertIn("mf_show", mock_print.call_args[0][0])

    def test_no_ipython_prints_message(self):
        from metaflow.plugins.jupyter.mf_magics import _mf_show

        with patch("metaflow.plugins.jupyter.mf_magics.IPython") as mock_ipy:
            mock_ipy.get_ipython.return_value = None
            with patch("builtins.print") as mock_print:
                _mf_show("self.df")
        mock_print.assert_called_once()


class TestLoadExtension(unittest.TestCase):
    def test_registers_magic_with_correct_name(self):
        from metaflow.plugins.jupyter.mf_magics import load_ipython_extension

        ip = MagicMock()
        load_ipython_extension(ip)
        ip.register_magic_function.assert_called_once()
        _, kwargs = ip.register_magic_function.call_args
        self.assertEqual(kwargs.get("magic_name"), "mf_show")
        self.assertEqual(kwargs.get("magic_kind"), "line")


if __name__ == "__main__":
    unittest.main()
