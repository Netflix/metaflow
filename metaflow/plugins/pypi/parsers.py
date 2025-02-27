# this file can be overridden by extensions as is (e.g. metaflow-nflx-extensions)
from metaflow.exception import MetaflowException


class ParserValueError(MetaflowException):
    headline = "Value error"


def requirements_txt_parser(content: str):
    """
    Parse non-comment lines from a requirements.txt file as strictly valid
    PEP 508 requirements.

    Recognizes direct references (e.g. "my_lib @ git+https://..."), extras
    (e.g. "requests[security]"), and version specifiers (e.g. "==2.0"). If
    the package name is "python", its specifier is stored in the "python"
    key instead of "packages".

    Parameters
    ----------
    content : str
        Contents of a requirements.txt file.

    Returns
    -------
    dict
        A dictionary with two keys:
            - "packages": dict(str -> str)
              Mapping from package name (plus optional extras/references) to a
              version specifier string.
            - "python": str or None
              The Python version constraints if present, otherwise None.

    Raises
    ------
     ParserValueError
        If a requirement line is invalid PEP 508 or if environment markers are
        detected, or if multiple Python constraints are specified.
    """
    import re
    from metaflow._vendor.packaging.requirements import Requirement, InvalidRequirement

    parsed = {"packages": {}, "python": None}

    inline_comment_pattern = re.compile(r"\s+#.*$")
    for line in content.splitlines():
        line = line.strip()

        # support Rye lockfiles by skipping lines not compliant with requirements
        if line == "-e file:.":
            continue

        if not line or line.startswith("#"):
            continue

        line = inline_comment_pattern.sub("", line).strip()
        if not line:
            continue

        try:
            req = Requirement(line)
        except InvalidRequirement:
            raise ParserValueError(f"Not a valid PEP 508 requirement: '{line}'")

        if req.marker is not None:
            raise ParserValueError(
                "Environment markers (e.g. 'platform_system==\"Linux\"') "
                f"are not supported for line: '{line}'"
            )

        dep_key = req.name
        if req.extras:
            dep_key += f"[{','.join(req.extras)}]"
        if req.url:
            dep_key += f"@{req.url}"

        dep_spec = str(req.specifier).lstrip(" =")

        if req.name.lower() == "python":
            if parsed["python"] is not None and dep_spec:
                raise ParserValueError(
                    f"Multiple Python version specs not allowed: '{line}'"
                )
            parsed["python"] = dep_spec or None
        else:
            parsed["packages"][dep_key] = dep_spec

    return parsed


def pyproject_toml_parser(content: str):
    """
    Parse a pyproject.toml file per PEP 621.

    Reads the 'requires-python' and 'dependencies' fields from the "[project]" section.
    Each dependency line must be a valid PEP 508 requirement. If the package name is
    "python", its specifier is stored in the "python" key instead of "packages".

    Parameters
    ----------
    content : str
        Contents of a pyproject.toml file.

    Returns
    -------
    dict
        A dictionary with two keys:
            - "packages": dict(str -> str)
              Mapping from package name (plus optional extras/references) to a
              version specifier string.
            - "python": str or None
              The Python version constraints if present, otherwise None.

    Raises
    ------
    RuntimeError
        If no TOML library (tomllib in Python 3.11+ or tomli in earlier versions) is found.
     ParserValueError
        If a dependency is not valid PEP 508, if environment markers are used, or if
        multiple Python constraints are specified.
    """
    try:
        import tomllib as toml  # Python 3.11+
    except ImportError:
        try:
            import tomli as toml  # Python < 3.11 (requires "tomli" package)
        except ImportError:
            raise RuntimeError(
                "Could not import a TOML library. For Python <3.11, please install 'tomli'."
            )
    from metaflow._vendor.packaging.requirements import Requirement, InvalidRequirement

    data = toml.loads(content)

    project = data.get("project", {})
    requirements = project.get("dependencies", [])
    requires_python = project.get("requires-python")

    parsed = {"packages": {}, "python": None}

    if requires_python is not None:
        # If present, store verbatim; note that PEP 621 does not necessarily
        # require "python" to be a dependency in the usual sense.
        # Example: "requires-python" = ">=3.7,<4"
        parsed["python"] = requires_python.lstrip("=").strip()

    for dep_line in requirements:
        dep_line_stripped = dep_line.strip()
        try:
            req = Requirement(dep_line_stripped)
        except InvalidRequirement:
            raise ParserValueError(
                f"Not a valid PEP 508 requirement: '{dep_line_stripped}'"
            )

        if req.marker is not None:
            raise ParserValueError(
                f"Environment markers not supported for line: '{dep_line_stripped}'"
            )

        dep_key = req.name
        if req.extras:
            dep_key += f"[{','.join(req.extras)}]"
        if req.url:
            dep_key += f"@{req.url}"

        dep_spec = str(req.specifier).lstrip("=")

        if req.name.lower() == "python":
            if parsed["python"] is not None and dep_spec:
                raise ParserValueError(
                    f"Multiple Python version specs not allowed: '{dep_line_stripped}'"
                )
            parsed["python"] = dep_spec or None
        else:
            parsed["packages"][dep_key] = dep_spec

    return parsed


def conda_environment_yml_parser(content: str):
    """
    Parse a minimal environment.yml file under strict assumptions.

    The file must contain a 'dependencies:' line, after which each dependency line
    appears with a '- ' prefix. Python can appear as 'python=3.9', etc.; other
    packages as 'numpy=1.21.2' or simply 'numpy'. Non-compliant lines raise  ParserValueError.

    Parameters
    ----------
    content : str
        Contents of a environment.yml file.

    Returns
    -------
    dict
        A dictionary with keys:
        {
            "packages": dict(str -> str),
            "python": str or None
        }

    Raises
    ------
     ParserValueError
        If the file has malformed lines or unsupported sections.
    """
    import re

    packages = {}
    python_version = None

    inside_dependencies = False

    # Basic pattern for lines like "numpy=1.21.2"
    # Group 1: package name
    # Group 2: optional operator + version (could be "=1.21.2", "==1.21.2", etc.)
    line_regex = re.compile(r"^([A-Za-z0-9_\-\.]+)(\s*[=<>!~].+\s*)?$")
    inline_comment_pattern = re.compile(r"\s+#.*$")

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        line = inline_comment_pattern.sub("", line).strip()
        if not line:
            continue

        if line.lower().startswith("dependencies:"):
            inside_dependencies = True
            continue

        if inside_dependencies and not line.startswith("-"):
            inside_dependencies = False
            continue

        if not inside_dependencies:
            continue

        dep_line = line.lstrip("-").strip()
        if dep_line.endswith(":"):
            raise ParserValueError(
                f"Unsupported subsection '{dep_line}' in environment.yml."
            )

        match = line_regex.match(dep_line)
        if not match:
            raise ParserValueError(
                f"Line '{dep_line}' is not a valid conda package specifier."
            )

        pkg_name, pkg_version_part = match.groups()
        version_spec = pkg_version_part.strip() if pkg_version_part else ""

        if version_spec.startswith("="):
            version_spec = version_spec.lstrip("=").strip()

        if pkg_name.lower() == "python":
            if python_version is not None and version_spec:
                raise ParserValueError(
                    f"Multiple Python version specs detected: '{dep_line}'"
                )
            python_version = version_spec
        else:
            packages[pkg_name] = version_spec

    return {"packages": packages, "python": python_version}
