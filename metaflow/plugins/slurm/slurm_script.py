import uuid
from string import Template
from typing import Dict, Optional


SLURM_JOB_SCRIPT_TEMPLATE = """\
#!/bin/bash
{sbatch_directives}

{shell_env_setup}

# Create and enter job-specific directory
JOB_DIR="{job_dir_name}"

# Store the full path of the job directory
FULL_JOB_DIR="$(pwd)/assets/$JOB_DIR"

# Cleanup flag
CLEANUP="{cleanup}"

if [ "$CLEANUP" = "true" ]; then
    # Function to clean up the job directory
    cleanup() {{
        echo "Cleaning up job directory... $FULL_JOB_DIR"
        rm -rf "$FULL_JOB_DIR"
    }}

    # Set trap to ensure cleanup happens on exit
    trap cleanup EXIT
fi

mkdir -p "$FULL_JOB_DIR"
cd "$FULL_JOB_DIR"

# Main job logic
main() {{
    set -e  # Exit immediately if a command exits with a non-zero status

    {run_commands}

    wait
}}

# Run main logic and capture exit code
main
exit_code=$?

# Exit with the code from the main logic
exit $exit_code
"""


class SlurmJobScript(object):
    def __init__(
        self,
        env: Optional[Dict[str, str]] = None,
        cleanup: bool = False,
        sbatch_options: Optional[Dict[str, str]] = None,
        srun_options: Optional[Dict[str, str]] = None,
        bashrc_path: Optional[str] = "$HOME/.bashrc",
    ):
        self.env_vars = env or {}
        self.cleanup = cleanup
        self.sbatch_options = sbatch_options or {}
        self.srun_options = srun_options or {}
        self.bashrc_path = bashrc_path

    @property
    def sbatch_directives(self):
        directives = []
        for key, value in self.sbatch_options.items():
            if len(key) == 1:
                directives.append(
                    "#SBATCH -%s%s" % (key, " %s" % value if value else "")
                )
            else:
                directives.append(
                    "#SBATCH --%s%s" % (key, "=%s" % value if value else "")
                )

        return "\n".join(directives)

    @property
    def srun_args(self):
        srun_options = []
        for key, value in self.srun_options.items():
            if len(key) == 1:
                srun_options.append("-%s%s" % (key, " %s" % value if value else ""))
            else:
                srun_options.append("--%s%s" % (key, "=%s" % value if value else ""))

        return " ".join(srun_options)

    @property
    def shell_env_setup(self):
        setup_lines = [
            "source %s" % self.bashrc_path if self.bashrc_path else "",
        ]
        for key, value in self.env_vars.items():
            if key == "METAFLOW_INIT_SCRIPT":
                # this needs outer double quotes
                setup_lines.append('export %s="%s"' % (key, value))
            else:
                setup_lines.append("export %s='%s'" % (key, value))
        return "\n".join(setup_lines)

    def get_run_command(
        self,
        command: str,
    ) -> str:
        run_cmds = [
            "srun %s \\" % self.srun_args if self.srun_options else "srun \\",
            "  %s" % command,
        ]

        run_cmd = "\n".join(run_cmds)

        template = Template(run_cmd)
        run_cmd = template.safe_substitute(command=command)

        return run_cmd

    def generate_script(
        self,
        command: str,
    ):
        template_kwargs = {
            "sbatch_directives": self.sbatch_directives,
            "shell_env_setup": self.shell_env_setup,
            "run_commands": self.get_run_command(
                command=command,
            ),
            "job_dir_name": self.sbatch_options.get(
                "job-name", "job_%s" % uuid.uuid4().hex[:8]
            ),
            "cleanup": str(self.cleanup).lower(),
        }

        return SLURM_JOB_SCRIPT_TEMPLATE.format(**template_kwargs)


if __name__ == "__main__":
    sj = SlurmJobScript(
        env={"MADHUR": "COOL"},
        sbatch_options={
            "job-name": "python_job",
            "output": "python_job.out",
            "error": "python_job.err",
            "time": "00:10:00",
            "partition": "queue1",
            "nodes": "1",
            "ntasks": "1",
            "cpus-per-task": "4",
            "mem": "4G",
        },
    )

    print(
        sj.generate_script(
            command="python compute_np.py",
        )
    )
