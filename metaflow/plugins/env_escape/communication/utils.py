import socket


def __try_op__(op_name, op, retries, *args):
    """
    A helper function to retry an operation that timed out on a socket. After
    the retries are expired a `socket.timeout` is raised.

    Parameters
    ----------
    op_name : str
        The operations name
    op : Callable
        The operation to perform
    retries : int
        The number of retries
    args :
        Args for the operation

    Returns
    -------
    The operations response

    Raises
    ------
    socket.timeout
        If all retries are exhausted, `socket.timeout` is raised

    """
    for i in range(retries):
        try:
            result = op(*args)
            return result
        except socket.timeout:
            pass
    else:
        raise socket.timeout(
            "Timeout after {} retries on operation " "'{}'".format(retries, op_name)
        )
