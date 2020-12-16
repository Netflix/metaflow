from kfp import dsl

from metaflow import FlowSpec, step, kfp, current


def pipeler_op(run_id):
  return dsl.ContainerOp(
      name='pipelerstep1',
      image='analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py36-cpu-spark-jupyter:2.3.8254d0ef.spark-2-4',
      command=['sh', '-c'],
      arguments=[
        "spark-submit --master local --deploy-mode client --conf \"spark.eventLog.enabled=false\" "
        "--class com.zillow.pipeler.orchestrator.example.ExampleTemplatizedConfigStep "
        "https://artifactory.zgtools.net/artifactory/analytics-maven-local/com/zillow/pipeler/datalake-pipeler/2.3.0/datalake-pipeler-2.3.0.jar "
        f"--sessionId {run_id}_execution "
      ]
  )

class HelloPipeler(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """

    @step
    def start(self):
        """
        kfp.preceding_component_inputs Flow state ["who"] is passed to the KFP component as arguments
        """
        self.who = "world"
        self.run_id = current.run_id
        self.next(self.end)

    @kfp(
        preceding_component=pipeler_op,
        preceding_component_inputs="run_id"
    )
    @step
    def end(self):
        """
        kfp.preceding_component_outputs ["message"] is now available as Metaflow Flow state
        """
        print("who", self.who)


if __name__ == "__main__":
    HelloPipeler()
