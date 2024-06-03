import sys
from typing import Optional, ClassVar
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows
from metaflow.plugins.argo.argo_workflows_cli import get_status_considering_run_object
from metaflow.plugins.deployer import (
    Deployer,
    DeployedFlow,
    TriggeredRun,
    get_lower_level_group,
)


class ArgoExecutingRun(TriggeredRun):
    @property
    def status(self):
        flow_name, run_id = self.pathspec.split("/")
        name = run_id[5:]
        status = ArgoWorkflows.get_workflow_status(flow_name, name)
        if status is not None:
            return get_status_considering_run_object(status, self.run)
        return None

    def suspend(self, **kwargs):
        _, run_id = self.pathspec.split("/")
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.type,
            self.deployer.name,
        ).suspend(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0

    def unsuspend(self, **kwargs):
        _, run_id = self.pathspec.split("/")
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.type,
            self.deployer.name,
        ).unsuspend(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0


class ArgoWorkflowsTemplate(DeployedFlow):
    @property
    def production_token(self):
        _, production_token = ArgoWorkflows.get_existing_deployment(self.deployer.name)
        return production_token


class ArgoWorkflowsDeployer(Deployer):
    type: ClassVar[Optional[str]] = "argo-workflows"

    def create(self, **kwargs) -> DeployedFlow:
        command_obj = super().create(**kwargs)

        if command_obj.process.returncode == 0:
            return ArgoWorkflowsTemplate(deployer=self)

        raise Exception("Error deploying %s to %s" % (self.flow_file, self.type))

    def trigger(self, **kwargs) -> TriggeredRun:
        content, command_obj = super().trigger(**kwargs)

        if command_obj.process.returncode == 0:
            return ArgoExecutingRun(self, content)

        raise Exception(
            "Error triggering %s on %s for %s" % (self.name, self.type, self.flow_file)
        )


if __name__ == "__main__":
    import time

    ar = ArgoWorkflowsDeployer("../try.py")
    print(ar.name)
    ar_obj = ar.deploy()
    print(ar.name)
    print(type(ar))
    print(type(ar_obj))
    print(ar_obj.production_token)
    result = ar_obj.trigger(alpha=300)
    print(result.status)
    run = result.run
    while run is None:
        print("trying again...")
        run = result.run
    print(result.run)
    print(result.status)
    time.sleep(120)
    print(result.terminate())

    print("triggering from deployer..")
    result = ar.trigger(alpha=600)
    print(result.name)
    print(result.status)
    run = result.run
    while run is None:
        print("trying again...")
        run = result.run
    print(result.run)
    print(result.status)
    time.sleep(120)
    print(result.terminate())
