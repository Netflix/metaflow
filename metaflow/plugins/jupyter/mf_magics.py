"""Jupyter magic command ``%mf_show`` for rendering Metaflow artifacts inline.

Usage inside a notebook cell::

    %mf_show self.df        # pandas DataFrame → rich table
    %mf_show self.scores    # object with .plot() → matplotlib figure
    %mf_show self.model     # anything else → repr via display()

Load automatically after ``import metaflow``, or manually with::

    %load_ext metaflow.plugins.jupyter.mf_magics
"""

try:
    import IPython
    from IPython.display import display

    def _render(obj):
        """Display *obj* using the most appropriate renderer available."""
        try:
            import pandas as pd

            if isinstance(obj, pd.DataFrame):
                display(obj)
                return
        except ImportError:
            pass

        if hasattr(obj, "plot"):
            obj.plot()
        else:
            display(obj)

    def _mf_show(line):
        """Display a Metaflow artifact inline in the current notebook cell.

        Usage::

            %mf_show self.df
            %mf_show self.model
            %mf_show run.data.scores
        """
        ip = IPython.get_ipython()
        if ip is None:
            print("mf_show: not running inside IPython")
            return
        line = line.strip()
        if not line:
            print("Usage: %mf_show <expression>")
            return
        try:
            _render(ip.ev(line))
        except Exception as exc:  # noqa: BLE001
            print("mf_show: could not evaluate %r: %s" % (line, exc))

    def load_ipython_extension(ip):
        """Register ``%mf_show``; called by ``%load_ext metaflow``."""
        ip.register_magic_function(_mf_show, magic_kind="line", magic_name="mf_show")

    # Auto-register when Metaflow is imported inside a running IPython session.
    _ip = IPython.get_ipython()
    if _ip is not None:
        load_ipython_extension(_ip)

except ImportError:
    # IPython is not installed; silently provide a no-op stub.
    def load_ipython_extension(ip):  # noqa: F811
        pass
