from metaflow import FlowSpec, step, kubernetes, retry


class HelloCloudFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Metaflow says Hi from the cloud!'

    Run this flow to validate your Kubernetes configuration.

    """

    @step
    def start(self):
        """
        The 'start' step is a regular step, so runs locally on the machine from
        which the flow is executed.

        """
        from metaflow import get_metadata

        print("HelloCloud is starting.")
        print("")
        print("Using metadata provider: %s" % get_metadata())
        print("")
        print("The start step is running locally. Next, the ")
        print("'hello' step will run remotely on Kubernetes. ")

        self.next(self.hello)

    @kubernetes(cpu=1, memory=500)
    @retry
    @step
    def hello(self):
        """
        This steps runs remotely on Kubernetes using 1 virtual CPU and 500Mb of
        memory. Since we are now using a remote metadata service and data
        store, the flow information and artifacts are available from
        anywhere. The step also uses the retry decorator, so that if something
        goes wrong, the step will be automatically retried.

        """
        self.message = "Hi from the cloud!"
        print("Metaflow says: %s" % self.message)
        self.next(self.end)

    @step
    def end(self):
        """
        The 'end' step is a regular step, so runs locally on the machine from
        which the flow is executed.

        """
        print("HelloCloud is finished.")


if __name__ == "__main__":
    HelloCloudFlow()
