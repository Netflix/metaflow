# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import configparser
from os.path import basename, exists, join


def parse_python_version(ver_str):
    return tuple(int(digit) for digit in ver_str.split("."))


class NoFileError(Exception):
    pass


class FunctionalTestFile:
    """A single functional test case file with options."""

    _CONVERTERS = {
        "min_pyver": parse_python_version,
        "max_pyver": parse_python_version,
        "min_pyver_end_position": parse_python_version,
        "requires": lambda s: s.split(","),
    }

    def __init__(self, directory, filename):
        self._directory = directory
        self.base = filename.replace(".py", "")
        self.options = {
            "min_pyver": (2, 5),
            "max_pyver": (4, 0),
            "min_pyver_end_position": (3, 8),
            "requires": [],
            "except_implementations": [],
            "exclude_platforms": [],
        }
        self._parse_options()

    def __repr__(self):
        return f"FunctionalTest:{self.base}"

    def _parse_options(self):
        cp = configparser.ConfigParser()
        cp.add_section("testoptions")
        try:
            cp.read(self.option_file)
        except NoFileError:
            pass

        for name, value in cp.items("testoptions"):
            conv = self._CONVERTERS.get(name, lambda v: v)
            self.options[name] = conv(value)

    @property
    def option_file(self):
        return self._file_type(".rc")

    @property
    def module(self):
        package = basename(self._directory)
        return ".".join([package, self.base])

    @property
    def expected_output(self):
        return self._file_type(".txt", check_exists=False)

    @property
    def source(self):
        return self._file_type(".py")

    def _file_type(self, ext, check_exists=True):
        name = join(self._directory, self.base + ext)
        if not check_exists or exists(name):
            return name
        raise NoFileError(f"Cannot find '{name}'.")
