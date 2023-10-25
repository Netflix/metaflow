import sys
import inspect

INDENT = 4


class FlowFormatter(object):
    def __init__(self, graphspec, test):
        self.graphspec = graphspec
        self.test = test
        self.should_resume = getattr(test, "RESUME", False)
        self.should_fail = getattr(test, "SHOULD_FAIL", False)
        self.flow_name = "%sFlow" % self.test.__class__.__name__
        self.used = set()
        self._code_cache = {}
        self.steps = self._index_steps(test)
        self.flow_code = self._pretty_print(self._flow_lines())
        self.check_code = self._pretty_print(self._check_lines())
        self.valid = True

        for step in self.steps:
            if step.required and step not in self.used:
                self.valid = False

    def _format_method(self, step):
        def lines():
            lines, lineno = inspect.getsourcelines(step)
            lines_iter = iter(lines)
            is_next_line = False
            for line in lines_iter:
                head = line.lstrip()
                if is_next_line:
                    first_line = line
                    break
                if head.startswith("def "):
                    is_next_line = True
            indent = len(first_line) - len(first_line.lstrip())
            yield first_line[indent:].rstrip()
            for line in lines_iter:
                yield line[indent:].rstrip()

        code = self._code_cache.get(step)
        if code is None:
            code = self._code_cache[step] = list(lines())
        return code

    def _index_steps(self, test):
        steps = []
        for attr in dir(test):
            obj = getattr(test, attr)
            if hasattr(obj, "is_step"):
                steps.append(obj)
        return list(sorted(steps, key=lambda x: x.prio))

    def _node_quals(self, name, node):
        quals = {"all"}
        quals.update(node.get("quals", []))
        if name in ("start", "end"):
            quals.add(name)
        if "join" in node:
            quals.add("join")
        if "parallel_step" in node:
            quals.add("parallel-step")
        if "linear" in node:
            quals.add("linear")
        for qual in node.get("quals", []):
            quals.add(qual)
        return quals

    def _choose_step(self, name, node):
        node_quals = self._node_quals(name, node)
        for step in self.steps:
            if step.quals & node_quals:
                return step
        raise Exception(
            "Test %s doesn't have a match for step %s in graph %s"
            % (self.test, name, self.graphspec["name"])
        )

    def _flow_lines(self):

        tags = []
        for step in self.steps:
            tags.extend(tag.split("(")[0] for tag in step.tags)

        yield 0, "# -*- coding: utf-8 -*-"
        yield 0, "from metaflow import FlowSpec, step, Parameter, project, IncludeFile, JSONType, current, parallel"
        yield 0, "from metaflow_test import assert_equals, assert_equals_metadata, assert_exception, ExpectationFailed, is_resumed, ResumeFromHere, TestRetry, try_to_get_card"
        if tags:
            yield 0, "from metaflow import %s" % ",".join(tags)

        yield 0, self.test.HEADER
        yield 0, "class %s(FlowSpec):" % self.flow_name

        for var, val in self.test.CLASS_VARS.items():
            yield 1, "%s = %s" % (var, val)

        for var, parameter in self.test.PARAMETERS.items():
            kwargs = ["%s=%s" % (k, v) for k, v in parameter.items()]
            yield 1, '%s = Parameter("%s", %s)' % (var, var, ",".join(kwargs))

        for var, include in self.test.INCLUDE_FILES.items():
            kwargs = ["%s=%s" % (k, v) for k, v in include.items()]
            yield 1, '%s = IncludeFile("%s", %s)' % (var, var, ",".join(kwargs))

        for name, node in self.graphspec["graph"].items():
            step = self._choose_step(name, node)
            self.used.add(step)

            for tagspec in step.tags:
                yield 1, "@%s" % tagspec

            if "parallel_step" in node:
                yield 1, "@parallel"

            yield 1, "@step"

            if "join" in node:
                yield 1, "def %s(self, inputs):" % name
            else:
                yield 1, "def %s(self):" % name

            if "foreach" in node:
                yield 2, "self.%s = %s" % (
                    node["foreach_var"],
                    node["foreach_var_default"],
                )

            for line in self._format_method(step):
                yield 2, line

            if "linear" in node:
                yield 2, "self.next(self.%s)" % node["linear"]
            elif "branch" in node:
                branches = ",".join("self.%s" % x for x in node["branch"])
                yield 2, "self.next(%s)" % branches
            elif "foreach" in node:
                yield 2, 'self.next(self.%s, foreach="%s")' % (
                    node["foreach"],
                    node["foreach_var"],
                )
            elif "num_parallel" in node:
                yield 2, "self.next(self.%s, num_parallel=%d)" % (
                    node["parallel"],
                    node["num_parallel"],
                )

        yield 0, "if __name__ == '__main__':"
        yield 1, "%s()" % self.flow_name

    def _check_lines(self):
        yield 0, "# -*- coding: utf-8 -*-"
        yield 0, "import sys"
        yield 0, "from metaflow_test import assert_equals, assert_equals_metadata, assert_exception, new_checker"
        yield 0, "def check_results(flow, checker):"
        for line in self._format_method(self.test.check_results):
            yield 1, line
        yield 0, "if __name__ == '__main__':"
        yield 1, "from test_flow import %s" % self.flow_name
        yield 1, "flow = %s(use_cli=False)" % self.flow_name
        yield 1, "check = new_checker(flow)"
        yield 1, "check_results(flow, check)"

    def _pretty_print(self, lines):
        def _lines():
            for indent, line in lines:
                yield "".join((" " * (indent * INDENT), line))

        return "\n".join(_lines())

    def __str__(self):
        return "test '%s' graph '%s'" % (
            self.test.__class__.__name__,
            self.graphspec["name"],
        )
