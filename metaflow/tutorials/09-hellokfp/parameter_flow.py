import socket
from metaflow import FlowSpec, Parameter, step


def get_host_name(*arg) -> str:
    return socket.gethostname()


class ParameterFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    The hello step uses @resource decorator that only works when kfp plug-in is used.
    """

    alpha = Parameter(
        'alpha',
        help='param with default',
        default=0.01,
    )

    beta = Parameter(
        'beta',
        help='param with no default',
        type=int,
        required=True
    )

    host_name = Parameter(
        'host_name',
        help='Deploy-time param evaluated at deployment',
        type=str,
        default=get_host_name,
        required=True
    )

    @step
    def start(self):
        """
        All flows must have a step named 'start' that is the first step in the flow.
        """
        print(f"Alpha: {self.alpha}")
        print(f"Beta: {self.beta}")
        print(f"Host name: {self.host_name}")
        self.next(self.end)

    @step
    def end(self):
        """
        All flows must have an 'end' step, which is the last step in the flow.
        """
        print(f"Alpha: {self.alpha}")
        print(f"Beta: {self.beta}")
        print(f"Host name: {self.host_name}")


if __name__ == '__main__':
    ParameterFlow()
