from metaflow import FlowSpec, step, nomad, resources, current
import os

class HelloNomadFlow(FlowSpec):
    """
    A simple flow to demonstrate executing a step on HashiCorp Nomad.
    """

    @step
    def start(self):
        """
        Starts the flow.
        """
        print("HelloNomadFlow starting.")
        print("Next, we will run a step on Nomad.")
        self.next(self.nomad_step)

    @nomad(cpu=500, memory=256, disk=100) # Request 500 MHz CPU, 256MB RAM, 100MB Disk
    # Example of also using @resources decorator, though @nomad can specify directly
    @resources(memory=128) # @nomad's memory=256 will take precedence or be maxed
    @step
    def nomad_step(self):
        """
        This step is executed on Nomad.
        """
        print("Hello from a Nomad task!")
        print(f"This step is running as part of flow: {current.flow_name}")
        print(f"Run ID: {current.run_id}, Step: {current.step_name}, Task ID: {current.task_id}")

        # Check for an environment variable that should be set by the Nomad plugin
        nomad_workload_env = os.environ.get("METAFLOW_NOMAD_WORKLOAD")
        print(f"METAFLOW_NOMAD_WORKLOAD environment variable: {nomad_workload_env}")
        if nomad_workload_env == "1":
            print("Successfully identified as a Nomad workload via environment variable.")
        else:
            print("Warning: METAFLOW_NOMAD_WORKLOAD environment variable not found or incorrect.")

        self.message = "Successfully ran a step on Nomad!"
        print("Nomad step computation complete.")
        self.next(self.end)

    @step
    def end(self):
        """
        Ends the flow.
        """
        print(f"Back from Nomad. Message: {self.message}")
        print("HelloNomadFlow finished successfully!")

if __name__ == "__main__":
    HelloNomadFlow()
