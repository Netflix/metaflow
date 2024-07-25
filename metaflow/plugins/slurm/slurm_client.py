import re
import sys
import asyncio
import tempfile
from pathlib import Path
from typing import Optional
from metaflow.plugins.slurm.slurm_exceptions import SlurmException


_LOAD_SLURM_PREFIX = """\
source /etc/profile
module whatis slurm &> /dev/null
if [ $? -eq 0 ] ; then
  module load slurm
fi
"""


class SlurmClient(object):
    def __init__(
        self,
        username: Optional[str] = None,
        address: Optional[str] = None,
        ssh_key_file: Optional[str] = None,
        cert_file: Optional[str] = None,
        remote_workdir: Optional[str] = None,
        cleanup: bool = False,
    ):
        if not username:
            raise ValueError("username is a required parameter in the Slurm plugin.")
        if not address:
            raise ValueError("address is a required parameter in the Slurm plugin.")
        if not ssh_key_file:
            raise ValueError(
                "ssh_key_file is a required parameter in the Slurm plugin."
            )

        try:
            self.asyncssh = __import__("asyncssh")
        except (NameError, ImportError, ModuleNotFoundError):
            raise SlurmException(
                "Could not import module 'asyncssh'.\n\nInstall asyncssh "
                "Python package (https://pypi.org/project/asyncssh/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install asyncssh\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

        ssh_key_file_path = Path(ssh_key_file).expanduser().resolve()
        cert_file_path = Path(cert_file).expanduser().resolve() if cert_file else None

        if cert_file and not cert_file_path.exists():
            raise FileNotFoundError("Certificate file not found: %s" % cert_file)
        if not ssh_key_file_path.exists():
            raise FileNotFoundError("SSH key file not found: %s" % ssh_key_file)
        if cert_file:
            self.client_keys = [
                (
                    self.asyncssh.read_private_key(ssh_key_file_path),
                    self.asyncssh.read_certificate(cert_file_path),
                )
            ]
        else:
            self.client_keys = [self.asyncssh.read_private_key(ssh_key_file_path)]

        self.username = username
        self.address = address
        self.conn = None

        self.remote_workdir = remote_workdir
        # TODO: need to use cleanup...
        self.cleanup = cleanup

    async def connect(self):
        try:
            self.conn = await self.asyncssh.connect(
                self.address,
                username=self.username,
                client_keys=self.client_keys,
                known_hosts=None,
            )

        except Exception as e:
            raise RuntimeError(
                "Could not connect to host: '%s' as user: '%s'"
                % (self.address, self.username)
            ) from e

        return self.conn

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    async def submit(self, job_name: str, slurm_script_contents: str):
        # make sure remote workdir exists..
        remote_workdir = Path(self.remote_workdir or "~")

        cmd_mkdir_remote = "mkdir -p %s" % remote_workdir
        proc_mkdir_remote = await self.conn.run(cmd_mkdir_remote)

        client_err = proc_mkdir_remote.stderr.strip()
        if client_err:
            raise RuntimeError(client_err)

        # copy the slurm script
        slurm_filename = "%s.sh" % job_name
        with tempfile.TemporaryDirectory() as temp_dir:
            slurm_script_file = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False, suffix=".sh"
            )
            filename = slurm_script_file.name
            remote_slurm_filename = Path(remote_workdir) / slurm_filename
            with open(filename, "w") as fp:
                fp.write(slurm_script_contents)
                fp.flush()

            await self.asyncssh.scp(
                filename,
                (self.address, remote_slurm_filename),
                username=self.username,
                client_keys=self.client_keys,
                known_hosts=None,
            )

        # run the slurm script through sbatch
        cmd_sbatch = "sbatch %s" % remote_slurm_filename
        proc_verify_sbatch = await self.conn.run(_LOAD_SLURM_PREFIX + "which sbatch")
        if proc_verify_sbatch.returncode != 0:
            raise RuntimeError("'sbatch' could not be found on the remote machine.")
        cmd_sbatch = _LOAD_SLURM_PREFIX + cmd_sbatch

        proc = await self.conn.run(cmd_sbatch)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())

        slurm_job_id = int(re.findall("[0-9]+", proc.stdout.strip())[0])
        return slurm_job_id

    async def terminate_job(self, job_id: str):
        cmd_scancel = "scancel %s" % job_id
        proc_verify_scancel = await self.conn.run(_LOAD_SLURM_PREFIX + "which scancel")
        if proc_verify_scancel.returncode != 0:
            raise RuntimeError("'scancel' could not be found on the remote machine.")
        cmd_scancel = _LOAD_SLURM_PREFIX + cmd_scancel

        proc = await self.conn.run(cmd_scancel)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())


async def main():

    sc = SlurmClient(
        username="ubuntu",
        address="18.236.81.10",
        ssh_key_file="~/Desktop/outerbounds/parallelcluster/madhur-slurm.pem",
    )
    await sc.connect()

    script_contents = """
#!/bin/bash
#SBATCH --job-name=python_job        # Job name
#SBATCH --output=python_job.out      # Standard output log
#SBATCH --error=python_job.err       # Standard error log
#SBATCH --time=00:10:00              # Walltime
#SBATCH --partition=queue1           # Partition name
#SBATCH --nodes=1                    # Number of nodes
#SBATCH --ntasks=1                   # Number of tasks
#SBATCH --cpus-per-task=4            # Number of CPU cores per task
#SBATCH --mem=4G                     # Memory per node

# Run the Python script
srun python compute_np.py
""".strip()

    jid = await sc.submit("madhur", script_contents)
    print("id: %s" % jid)


if __name__ == "__main__":
    asyncio.run(main())
