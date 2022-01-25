# Copyright (c) 2016-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2017 Hugo <hugovk@users.noreply.github.com>
# Copyright (c) 2018 Peter Talley <peterctalley@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Peter Pentchev <roam@ringlet.net>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Damien Baty <damien@damienbaty.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

import textwrap

from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.const import PY37_PLUS, PY39_PLUS
from metaflow._vendor.astroid.manager import AstroidManager


def _subprocess_transform():
    communicate = (bytes("string", "ascii"), bytes("string", "ascii"))
    communicate_signature = "def communicate(self, input=None, timeout=None)"
    args = """\
        self, args, bufsize=0, executable=None, stdin=None, stdout=None, stderr=None,
        preexec_fn=None, close_fds=False, shell=False, cwd=None, env=None,
        universal_newlines=False, startupinfo=None, creationflags=0, restore_signals=True,
        start_new_session=False, pass_fds=(), *, encoding=None, errors=None"""
    if PY37_PLUS:
        args += ", text=None"
    init = f"""
        def __init__({args}):
            pass"""
    wait_signature = "def wait(self, timeout=None)"
    ctx_manager = """
        def __enter__(self): return self
        def __exit__(self, *args): pass
    """
    py3_args = "args = []"

    if PY37_PLUS:
        check_output_signature = """
        check_output(
            args, *,
            stdin=None,
            stderr=None,
            shell=False,
            cwd=None,
            encoding=None,
            errors=None,
            universal_newlines=False,
            timeout=None,
            env=None,
            text=None,
            restore_signals=True,
            preexec_fn=None,
            pass_fds=(),
            input=None,
            bufsize=0,
            executable=None,
            close_fds=False,
            startupinfo=None,
            creationflags=0,
            start_new_session=False
        ):
        """.strip()
    else:
        check_output_signature = """
        check_output(
            args, *,
            stdin=None,
            stderr=None,
            shell=False,
            cwd=None,
            encoding=None,
            errors=None,
            universal_newlines=False,
            timeout=None,
            env=None,
            restore_signals=True,
            preexec_fn=None,
            pass_fds=(),
            input=None,
            bufsize=0,
            executable=None,
            close_fds=False,
            startupinfo=None,
            creationflags=0,
            start_new_session=False
        ):
        """.strip()

    code = textwrap.dedent(
        f"""
    def {check_output_signature}
        if universal_newlines:
            return ""
        return b""

    class Popen(object):
        returncode = pid = 0
        stdin = stdout = stderr = file()
        {py3_args}

        {communicate_signature}:
            return {communicate!r}
        {wait_signature}:
            return self.returncode
        def poll(self):
            return self.returncode
        def send_signal(self, signal):
            pass
        def terminate(self):
            pass
        def kill(self):
            pass
        {ctx_manager}
       """
    )
    if PY39_PLUS:
        code += """
    @classmethod
    def __class_getitem__(cls, item):
        pass
        """

    init_lines = textwrap.dedent(init).splitlines()
    indented_init = "\n".join(" " * 4 + line for line in init_lines)
    code += indented_init
    return parse(code)


register_module_extender(AstroidManager(), "subprocess", _subprocess_transform)
