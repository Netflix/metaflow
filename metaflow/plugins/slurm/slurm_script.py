from string import Template
from typing import Dict, Optional


SLURM_JOB_SCRIPT_TEMPLATE = """\
#!/bin/bash
{sbatch_directives}

{shell_env_setup}

{run_commands}

wait"""


class SlurmJobScript(object):
    def __init__(
        self,
        env: Optional[Dict[str, str]] = None,
        sbatch_options: Optional[Dict[str, str]] = None,
        srun_options: Optional[Dict[str, str]] = None,
        bashrc_path: Optional[str] = "$HOME/.bashrc",
    ):
        self.env_vars = env or {}
        self.sbatch_options = sbatch_options or {}
        self.srun_options = srun_options or {}
        self.bashrc_path = bashrc_path

    @property
    def sbatch_directives(self):
        directives = []
        for key, value in self.sbatch_options.items():
            if len(key) == 1:
                directives.append(f"#SBATCH -{key}" + (f" {value}" if value else ""))
            else:
                directives.append(f"#SBATCH --{key}" + (f"={value}" if value else ""))

        return "\n".join(directives)

    @property
    def srun_args(self):
        srun_options = []
        for key, value in self.srun_options.items():
            if len(key) == 1:
                srun_options.append(f"-{key}" + (f" {value}" if value else ""))
            else:
                srun_options.append(f"--{key}" + (f"={value}" if value else ""))

        return " ".join(srun_options)

    @property
    def shell_env_setup(self):
        setup_lines = [
            f"source {self.bashrc_path}" if self.bashrc_path else "",
        ]
        setup_lines.extend(
            f'export {key}="{value}"' for key, value in self.env_vars.items()
        )
        return "\n".join(setup_lines)

    def get_run_command(
        self,
        command: str,
    ) -> str:
        run_cmds = [
            f"srun {self.srun_args} \\" if self.srun_options else "srun \\",
            f"  {command}",
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
