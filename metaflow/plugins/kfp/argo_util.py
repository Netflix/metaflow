import shutil
import subprocess
import warnings

from kfp.components._yaml_utils import dump_yaml


argo_path = shutil.which("argo")


def validate_workflow(workflow: dict, kinds: str = "workflows"):
    if argo_path:
        has_working_argo_lint = False
        try:
            has_working_argo_lint = run_argo_lint(sample_argo_workflow, "workflows")
        except:
            warnings.warn(
                "Cannot validate the compiled workflow."
                "Found the argo program in PATH, but it's not usable."
                "Argo CLI v3.1.1+ should work."
            )

        if has_working_argo_lint:
            run_argo_lint(dump_yaml(workflow), kinds=kinds)


def run_argo_lint(yaml_text: str, kinds: str):
    # Running Argo lint if available
    if argo_path:
        result = subprocess.run(
            [
                argo_path,
                "lint",
                "--offline=true",
                f"--kinds={kinds}",
                "/dev/stdin",
            ],
            input=yaml_text.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode:
            raise RuntimeError("Invalid Argo Format")
        return True
    return False


sample_argo_workflow = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: hello-world-
spec:
  entrypoint: whalesay
  templates:
  - name: whalesay
    container:
      image: docker/whalesay:latest
"""
