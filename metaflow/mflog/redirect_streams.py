import os
import sys
import subprocess
import threading

from .mflog import decorate

# This script runs another process and captures stderr and stdout to a file, decorating
# lines with mflog metadata.
#
# Usage: redirect_streams SOURCE STDOUT_FILE STDERR_FILE PROGRAM ARG1 ARG2 ...


def reader_thread(SOURCE, dest_file, dest_stream, src):
    with open(dest_file, mode="ab", buffering=0) as f:
        if sys.version_info < (3, 0):
            # Python 2
            for line in iter(sys.stdin.readline, ""):
                # https://bugs.python.org/issue3907
                decorated = decorate(SOURCE, line)
                f.write(decorated)
                sys.stdout.write(line)
        else:
            # Python 3
            for line in src:
                decorated = decorate(SOURCE, line)
                f.write(decorated)
                dest_stream.buffer.write(line)


if __name__ == "__main__":
    SOURCE = sys.argv[1].encode("utf-8")
    stdout_dest = sys.argv[2]
    stderr_dest = sys.argv[3]

    p = subprocess.Popen(
        sys.argv[4:],
        env=os.environ,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout_reader = threading.Thread(
        target=reader_thread, args=(SOURCE, stdout_dest, sys.stdout, p.stdout)
    )
    stdout_reader.start()
    stderr_reader = threading.Thread(
        target=reader_thread, args=(SOURCE, stderr_dest, sys.stderr, p.stderr)
    )
    stderr_reader.start()
    rc = p.wait()
    stdout_reader.join()
    stderr_reader.join()
    sys.exit(rc)
