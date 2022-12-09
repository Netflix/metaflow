import heapq
import re
import time
import uuid

from datetime import datetime
from collections import namedtuple
from metaflow.util import to_bytes, to_fileobj, to_unicode

VERSION = b"0"

RE = rb"(\[!)?" rb"\[MFLOG\|" rb"(0)\|" rb"(.+?)Z\|" rb"(.+?)\|" rb"(.+?)\]" rb"(.*)"

# the RE groups defined above must match the MFLogline fields below
# except utc_timestamp, which is filled in by the parser based on utc_tstamp_str
MFLogline = namedtuple(
    "MFLogline",
    [
        "should_persist",
        "version",
        "utc_tstamp_str",
        "logsource",
        "id",
        "msg",
        "utc_tstamp",
    ],
)

LINE_PARSER = re.compile(RE)

ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"

MISSING_TIMESTAMP = datetime(3000, 1, 1)
MISSING_TIMESTAMP_STR = MISSING_TIMESTAMP.strftime(ISOFORMAT)

# utc_to_local() is based on https://stackoverflow.com/a/13287083
# NOTE: it might not work correctly for historical timestamps, e.g.
# if timezone definitions have changed. It should be ok for recently
# generated timestamps.
if time.timezone == 0:
    # the local timezone is UTC (common on servers). Don't waste time
    # on conversions
    utc_to_local = lambda x: x
else:
    try:
        # python3
        from datetime import timezone

        def utc_to_local(utc_dt):
            return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

    except ImportError:
        # python2
        import calendar

        def utc_to_local(utc_dt):
            timestamp = calendar.timegm(utc_dt.timetuple())
            local_dt = datetime.fromtimestamp(timestamp)
            return local_dt.replace(microsecond=utc_dt.microsecond)


def decorate(source, line, version=VERSION, now=None, lineid=None):
    if now is None:
        now = datetime.utcnow()
    tstamp = to_bytes(now.strftime(ISOFORMAT))
    if not lineid:
        lineid = to_bytes(str(uuid.uuid4()))
    line = to_bytes(line)
    source = to_bytes(source)
    return b"".join(
        (b"[MFLOG|", version, b"|", tstamp, b"Z|", source, b"|", lineid, b"]", line)
    )


def is_structured(line):
    line = to_bytes(line)
    return line.startswith(b"[MFLOG|") or line.startswith(b"[![MFLOG|")


def parse(line):
    line = to_bytes(line)
    m = LINE_PARSER.match(to_bytes(line))
    if m:
        try:
            fields = list(m.groups())
            fields.append(datetime.strptime(to_unicode(fields[2]), ISOFORMAT))
            return MFLogline(*fields)
        except:
            pass


def set_should_persist(line):
    # this marker indicates that the logline should be persisted by
    # the receiver
    line = to_bytes(line)
    if is_structured(line) and not line.startswith(b"[!["):
        return b"[!" + line
    else:
        return line


def unset_should_persist(line):
    # prior to persisting, the should_persist marker should be removed
    # from the logline using this function
    line = to_bytes(line)
    if is_structured(line) and line.startswith(b"[!["):
        return line[2:]
    else:
        return line


def refine(line, prefix=None, suffix=None):
    line = to_bytes(line)
    prefix = to_bytes(prefix) if prefix else b""
    suffix = to_bytes(suffix) if suffix else b""
    parts = line.split(b"]", 1)
    if len(parts) == 2:
        header, body = parts
        return b"".join((header, b"]", prefix, body, suffix))
    else:
        return line


def merge_logs(logs):
    def line_iter(logblob):
        # all valid timestamps are guaranteed to be smaller than
        # MISSING_TIMESTAMP, hence this iterator maintains the
        # ascending order even when corrupt loglines are present
        missing = []
        for line in to_fileobj(logblob):
            res = parse(line)
            if res:
                yield res.utc_tstamp_str, res
            else:
                missing.append(line)
        for line in missing:
            res = MFLogline(
                False, None, MISSING_TIMESTAMP_STR, None, None, line, MISSING_TIMESTAMP
            )
            yield res.utc_tstamp_str, res

    # note that sorted() below should be a very cheap, often a O(n) operation
    # because Python's Timsort is very fast for already sorted data.
    for _, line in heapq.merge(*[sorted(line_iter(blob)) for blob in logs]):
        yield line
