import sys
from .mflog import decorate

# This script is similar to the command-line utility 'tee':
# It reads stdin line by line and writes the lines to stodut
# and a file. In contrast to 'tee', this script formats each
# line with mflog-style structure.

if __name__ == '__main__':
    SOURCE = sys.argv[1].encode('ascii')
    with open(sys.argv[2], mode='ab', buffering=0) as f:
        for line in sys.stdin.buffer:
            decorated = decorate(SOURCE, line)
            f.write(decorated)
            sys.stdout.buffer.write(line)