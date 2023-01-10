import io

from metaflow.metaflow_config import EVENT_SOURCE_URL
from metaflow.current import current
from metaflow.exception import MetaflowException


class BadEventConfig(MetaflowException):
    headline = "Event configuration error"


STATUS_TENSES = {"succeeded": "succeeds", "failed": "fails"}


def event_topic():
    if EVENT_SOURCE_URL is None:
        raise BadEventConfig(msg="Required config entry EVENT_SOURCE_URL is missing.")
    chunks = EVENT_SOURCE_URL.split("//")
    if len(chunks) < 2:
        raise BadEventConfig(msg="Unknown EVENT_SOURCE_URL format.")
    url_chunks = chunks[1].split("/")
    if len(url_chunks) < 2:
        raise BadEventConfig(msg="EVENT_SOURCE_URL missing topic.")
    return url_chunks[-1]


def current_flow_name():
    flow_name = current.get("project_flow_name")
    if flow_name is None:
        flow_name = current.flow_name
    return flow_name.lower()


def project_and_branch():
    return (current.get("project_name"), current.get("branch_name"))


# Creates the Argo Event sensor name for a given workflow
def format_sensor_name(flow_name):
    # Argo's internal NATS bus throws an error when attempting to
    # create a persistent queue for the sensor when the sensor name
    # contains '.' which is NATS' topic delimiter.
    return flow_name.replace(".", "-").lower()


def list_to_prose(items, singular, formatter=None, use_quotes=False, plural=None):
    if formatter is not None:
        items = [formatter(item) for item in items]
    item_count = len(items)
    if plural is None:
        plural = singular + "s"
    item_type = singular
    if item_count == 1:
        if use_quotes:
            template = "'%s'"
        else:
            template = "%s"
        result = template % items[0]
    elif item_count == 2:
        if use_quotes:
            template = "'%s' and '%s'"
        else:
            template = "%s and %s"
        result = template % (items[0], items[1])
        item_type = plural
    elif item_count > 2:
        if use_quotes:
            formatted = ", ".join(
                ["'" + item + "'" for item in items[0 : item_count - 1]]
            )
            result = "%s and '%s'" % (formatted, items[item_count - 1])
        else:
            formatted = ", ".join(items[0 : item_count - 1])
            result = "%s and %s" % (formatted, items[item_count - 1])
            item_type = plural
    else:
        result = ""
    return (item_type, result)


class SourceCodeBuffer:
    def __init__(self):
        self.imports = []
        self.debug_counter = 1
        self.buf = io.StringIO(newline="\n")

    def add_imports(self, import_names):
        self.imports += import_names

    def add_line(self, line, indent=0):
        indenting = ""
        for i in range(0, indent):
            indenting += "    "
        self.buf.write(indenting)
        self.buf.write(line)
        self.buf.write("\n")

    def getvalue(self, line_numbers=False):
        final = io.StringIO()
        for i in self.imports:
            final.write("import %s\n" % i)
        final.write("\n")
        final.write(self.buf.getvalue())
        if not line_numbers:
            return final.getvalue()
        lines = final.getvalue().splitlines(keepends=True)
        n = 1
        updated = []
        for line in lines:
            updated.append("%d: %s" % (n, line))
            n += 1
        return "".join(updated)
