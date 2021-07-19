class ByteStream(object):
    """Basic interface that reads and writes bytes"""

    def read(self, count, timeout=None):
        """
        Reads exactly count bytes from the stream. This call is blocking until count bytes
        are read or an error happens

        This call returns a byte array or EOFError if there was a problem
        reading.

        Parameters
        ----------
        count : int
            Exact number of characters to read

        Returns
        -------
        bytes
            Content read from the stream

        Raises
        ------
        EOFError
            Any issue with reading will be raised as a EOFError
        """
        raise NotImplementedError

    def write(self, data):
        """
        Writes all the data to the stream

        This call is blocking until all data is written. EOFError will be
        raised if there is a problem writing to the stream

        Parameters
        ----------
        data : bytes
            Data to write out

        Raises
        ------
        EOFError
            Any issue with writing will be raised as a EOFError
        """
        raise NotImplementedError

    def close(self):
        """
        Closes the stream releasing all system resources

        Once closed, the stream cannot be re-opened or re-used. If a
        stream is already closed, this operation will have no effect
        """
        raise NotImplementedError()

    @property
    def is_closed(self):
        """
        Returns True if the stream is closed or False otherwise

        Returns
        -------
        bool
            True if closed or False otherwise
        """
        raise NotImplementedError()

    def fileno(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
