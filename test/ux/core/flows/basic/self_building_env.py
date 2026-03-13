import subprocess
import sys
import tempfile

from metaflow import (
    FlowSpec,
    Parameter,
    conda,
    current,
    named_env,
    project,
    step,
)


def trigger_name_func(context):
    return [current.project_flow_name + "Trigger"]


@project(name="selfbuild")
class BuildCondaEnvInStep(FlowSpec):
    my_var = Parameter(
        "my_var",
        default=123,
        external_artifact=trigger_name_func,
        external_trigger=True,
    )

    @conda(disabled=True)
    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        full_run_id = current.run_id
        if "-" in full_run_id:
            base_run_id = full_run_id.split("-")[1]
        else:
            base_run_id = full_run_id
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as req_file:
            req_file.write("itsdangerous==2.1.2")
            req_file.flush()
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "metaflow.cmd.main_cli",
                    "environment",
                    "resolve",
                    "--alias",
                    "mlp/metaflow/test/build_in_step/id_%s_%s_%s"
                    % (current.run_id, base_run_id, str(self.my_var)),
                    "-r",
                    req_file.name,
                    "--python",
                    "3.8.*",
                ]
            )
        print(
            "Build environment and aliased using mlp/metaflow/test/build_in_step/id_%s_%s_%s"
            % (current.run_id, base_run_id, str(self.my_var))
        )
        self.next(self.fetch_old)

    @conda(
        name="mlp/metaflow/test/build_in_step/id_@{METAFLOW_RUN_ID}_@{METAFLOW_RUN_ID_BASE}_@{METAFLOW_INIT_MY_VAR}",
        fetch_at_exec=True,
    )
    @step
    def fetch_old(self):
        import itsdangerous

        print("Imported itsdangerous and found version %s" % itsdangerous.__version__)
        self.found_version_old = itsdangerous.__version__
        self.next(self.end)

    @named_env(
        name="mlp/metaflow/test/build_in_step/id_@{METAFLOW_RUN_ID}_@{METAFLOW_RUN_ID_BASE}_@{METAFLOW_INIT_MY_VAR}",
        fetch_at_exec=True,
    )
    @step
    def end(self):
        import itsdangerous

        print("Imported itsdangerous and found version %s" % itsdangerous.__version__)
        self.found_version = itsdangerous.__version__


if __name__ == "__main__":
    BuildCondaEnvInStep()
