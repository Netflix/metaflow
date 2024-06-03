from typing import Optional, ClassVar
from metaflow.plugins.aws.step_functions.step_functions import StepFunctions
from metaflow.plugins.deployer import (
    Deployer,
    DeployedFlow,
    TriggeredRun,
)


class StepFunctionExecutingRun(TriggeredRun):
    @property
    def status(self):
        raise NotImplementedError


class StepFunctionsStateMachine(DeployedFlow):
    @property
    def production_token(self):
        _, production_token = StepFunctions.get_existing_deployment(self.deployer.name)
        return production_token


class StepFunctionsDeployer(Deployer):
    type: ClassVar[Optional[str]] = "step-functions"

    def create(self, **kwargs) -> DeployedFlow:
        command_obj = super().create(**kwargs)

        if command_obj.process.returncode == 0:
            return StepFunctionsStateMachine(deployer=self)

        raise Exception("Error deploying %s to %s" % (self.flow_file, self.type))

    def trigger(self, **kwargs) -> TriggeredRun:
        content, command_obj = super().trigger(**kwargs)

        if command_obj.process.returncode == 0:
            return StepFunctionExecutingRun(self, content)

        raise Exception(
            "Error triggering %s on %s for %s" % (self.name, self.type, self.flow_file)
        )


if __name__ == "__main__":
    import time

    ar = StepFunctionsDeployer("../try.py")
    print(ar.name)
    ar_obj = ar.deploy()
    print(ar.name)
    print(type(ar))
    print(type(ar_obj))
    print(ar_obj.production_token)
    result = ar_obj.trigger(alpha=300)
    run = result.run
    while run is None:
        print("trying again...")
        run = result.run
    print(result.run)
    time.sleep(120)
    print(result.terminate())

    print("triggering from deployer..")
    result = ar.trigger(alpha=600)
    print(result.name)
    run = result.run
    while run is None:
        print("trying again...")
        time.sleep(5)
        run = result.run
    print(result.run)
    time.sleep(120)
    print(result.terminate())
