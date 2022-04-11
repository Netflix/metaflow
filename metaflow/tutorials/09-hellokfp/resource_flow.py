from metaflow import FlowSpec, step, resources


class ResourceFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    The hello step uses @resource decorator that only works when kfp plug-in is used.
    """

    @step
    def start(self):
        """
        All flows must have a step named 'start' that is the first step in the flow.
        """
        print("\n\n_______________________________________")
        print("START step:\n We are now inside the START step - Hello World!\n")
        print("___________________________________________")
        self.next(self.all_resource)

    @resources(
        cpu=0.5, cpu_limit=5, gpu=3, gpu_vendor="amd", memory=150, memory_limit="1G"
    )
    @step
    def all_resource(self):
        """
        A step where all resource requirements keys are used.
        """
        print("\n\n_______________________________________")
        print("All resource requirements are set by resource decorator.")
        print("All resource requests and limits are expected in argo yaml.")
        print("___________________________________________")
        self.next(self.no_resource)

    @resources
    @step
    def no_resource(self):
        """
        A step where resource decorator was used but no attribute was set
        """
        print("\n\n_______________________________________")
        print("Resource decorator sets no resource requirements.")
        print("No resource limit or request expected in argo yaml.")
        print("___________________________________________")
        self.next(self.no_decorator)

    @step
    def no_decorator(self):
        """
        A step where resource decorator was not used
        """
        print("\n\n_______________________________________")
        print("No resource decorator was used.")
        print("No resource limit or request expected in argo yaml.")
        print("___________________________________________")
        self.next(self.end)

    @step
    def end(self):
        """
        All flows must have an 'end' step, which is the last step in the flow.
        """
        print("\n\n_______________________________________")
        print("END step:\n We have reached the END step!\n")
        print("ResourceFlow is all done.")
        print("___________________________________________")


if __name__ == "__main__":
    ResourceFlow()
