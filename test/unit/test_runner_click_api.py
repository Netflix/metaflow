import json
import os
import subprocess
import sys
import textwrap


def _run_python_snippet(snippet):
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        repo_root
        if not env.get("PYTHONPATH")
        else os.pathsep.join([repo_root, env["PYTHONPATH"]])
    )
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(snippet)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip())


def test_click_api_import_does_not_eagerly_load_typeguard():
    result = _run_python_snippet(
        """
        import json
        import sys
        import metaflow.runner.click_api  # noqa: F401

        print(
            json.dumps(
                {
                    "typeguard": "metaflow._vendor.typeguard" in sys.modules,
                    "typeguard_v3_7": "metaflow._vendor.v3_7.typeguard" in sys.modules,
                }
            )
        )
        """
    )
    assert result == {"typeguard": False, "typeguard_v3_7": False}


def test_click_api_loads_typeguard_when_type_checking():
    result = _run_python_snippet(
        """
        import json
        import sys
        from collections import OrderedDict
        from metaflow._vendor import click
        from metaflow.runner.click_api import _method_sanity_check

        before = (
            "metaflow._vendor.typeguard" in sys.modules
            or "metaflow._vendor.v3_7.typeguard" in sys.modules
        )

        _method_sanity_check(
            OrderedDict(),
            OrderedDict(
                [("quiet", click.Option(["--quiet"], is_flag=True, default=False))]
            ),
            OrderedDict([("quiet", bool)]),
            OrderedDict([("quiet", False)]),
            quiet=True,
        )

        after = (
            "metaflow._vendor.typeguard" in sys.modules
            or "metaflow._vendor.v3_7.typeguard" in sys.modules
        )

        print(json.dumps({"before": before, "after": after}))
        """
    )
    assert result == {"before": False, "after": True}
