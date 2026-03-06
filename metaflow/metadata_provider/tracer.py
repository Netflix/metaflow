"""
MetadataTracer — opt-in request tracing for the Metaflow Client.

Usage::

    from metaflow import Run
    from metaflow.metadata_provider.tracer import MetadataTracer

    with MetadataTracer() as tracer:
        run = Run("MyFlow/123")
        failed = [t for s in run for t in s if not t.successful]

    print(tracer.summary())
    # → {"total": 49, "by_type": {"run": 1, "step": 1, "task": 8}}

    for call in tracer.calls:
        print(call)

Notes
-----
* Tracing is **disabled** by default.  When no tracer is active the only
  overhead inside ``get_object()`` is a single ``is None`` check.
* The tracer stores state at class level on ``MetadataProvider``.  It is
  therefore **not** safe for concurrent use from multiple threads; a tracer
  captures calls from all threads that share the same process.  For
  single-threaded benchmark scripts this is the correct behaviour; if
  thread-safety is needed, switch ``_tracer`` to a ``contextvars.ContextVar``.
"""

import time
from collections import Counter


class MetadataTracer:
    """Context manager that records every ``MetadataProvider.get_object`` call.

    Attributes
    ----------
    calls : list[dict]
        Ordered list of trace records.  Each record contains:

        ``obj_type``    – the object type being fetched  (e.g. ``"run"``)
        ``sub_type``    – the sub-type / aggregation      (e.g. ``"step"``)
        ``depth``       – numeric hierarchy depth (root=0 … artifact=5)
        ``path``        – slash-joined positional path args (e.g. ``"MyFlow/42"``)
        ``attempt``     – attempt number, or ``None``
        ``ts``          – wall-clock timestamp at call start (``time.time()``)
        ``elapsed_ms``  – wall-clock duration of the backend call in milliseconds
        ``error``       – exception instance if the call failed, else ``None``
    """

    def __init__(self):
        self.calls = []
        self._previous_tracer = None

    # ------------------------------------------------------------------
    # Internal

    def _record(self, obj_type, sub_type, depth, attempt, path_args, ts, elapsed_ms, error=None):
        self.calls.append(
            {
                "obj_type": obj_type,
                "sub_type": sub_type,
                "depth": depth,
                "path": "/".join(str(a) for a in path_args if a is not None),
                "attempt": attempt,
                "ts": ts,
                "elapsed_ms": elapsed_ms,
                "error": error,
            }
        )

    # ------------------------------------------------------------------
    # Context-manager protocol

    def __enter__(self):
        # Import here to avoid a circular import at module load time.
        from metaflow.metadata_provider.metadata import MetadataProvider

        self._previous_tracer = MetadataProvider._tracer
        MetadataProvider._tracer = self
        return self

    def __exit__(self, *_):
        from metaflow.metadata_provider.metadata import MetadataProvider

        MetadataProvider._tracer = self._previous_tracer

    # ------------------------------------------------------------------
    # Reporting helpers

    def summary(self):
        """Return a compact summary of all recorded calls.

        Returns
        -------
        dict
            ``total``   – total number of ``get_object`` calls recorded
            ``by_type`` – mapping of *obj_type* → call count
        """
        counts = Counter(c["obj_type"] for c in self.calls)
        return {"total": len(self.calls), "by_type": dict(counts)}

    def report(self):
        """Print a human-readable breakdown to stdout."""
        s = self.summary()
        print("=" * 68)
        print(f"  MetadataTracer — {s['total']} total get_object calls")
        print("=" * 68)
        print(f"  {'obj_type':<12} {'sub_type':<12} {'depth':>5}  {'elapsed_ms':>10}  {'status':<8}  path")
        print("  " + "-" * 64)
        for c in self.calls:
            status = "ERROR" if c["error"] is not None else "ok"
            elapsed = f"{c['elapsed_ms']:.1f}" if c["elapsed_ms"] is not None else "?"
            print(
                f"  {c['obj_type']:<12} {c['sub_type']:<12} {c['depth']:>5}  {elapsed:>10}  {status:<8}  {c['path']}"
            )
        print("=" * 68)
        print("  Calls by type:")
        for typ, cnt in sorted(s["by_type"].items(), key=lambda x: -x[1]):
            print(f"    {typ:<12} {cnt}")
        print("=" * 68)
