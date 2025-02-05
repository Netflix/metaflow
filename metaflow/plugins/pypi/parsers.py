import re
from typing import Any, Dict, List, Optional

REQ_SPLIT_LINE = re.compile(r"([^~<=>]*)([~<=>]+.*)?")

# Allows things like:
# pkg = <= version
# pkg <= version
# pkg = version
# pkg = ==version or pkg = =version
# In other words, the = is optional but possible
YML_SPLIT_LINE = re.compile(r"(?:=\s)?(<=|>=|~=|==|<|>|=)")


def req_parser(config_value: str) -> Dict[str, Any]:
    extra_args = {}
    sources = {}
    deps = {}
    np_deps = {}
    sys_deps = {}
    python_version = parse_req_value(
        config_value, extra_args, sources, deps, np_deps, sys_deps
    )
    result = {}
    if python_version:
        result["python"] = python_version

    if extra_args:
        raise ValueError(
            "Additional arguments are not supported when parsing requirements.txt for "
            "the pypi decorator -- use Netflix's Metaflow extensions (metaflow-netflixext)'s "
            "named environment instead"
        )
    if np_deps:
        raise ValueError(
            "Non-python dependencies are not supported when parsing requirements.txt for "
            "the pypi decorator -- use Netflix's Metaflow extensions (metaflow-netflixext)'s "
            "named environment instead"
        )
    if sys_deps:
        raise ValueError(
            "System dependencies are not supported when parsing requirements.txt for "
            "the pypi decorator -- use Netflix's Metaflow extensions (metaflow-netflixext)'s "
            "named environment instead"
        )

    if sources:
        raise ValueError(
            "Specifying extra indices is not supported. Include those sources "
            "directly in your pip.conf or use Netflix's Metaflow extensions "
            "(metaflow-netflixext)"
        )

    result["packages"] = deps

    return result


def yml_parser(config_value: str) -> Dict[str, Any]:
    sources = {}
    conda_deps = {}
    pypi_deps = {}
    sys_deps = {}
    python_version = parse_yml_value(
        config_value, {}, sources, conda_deps, pypi_deps, sys_deps
    )
    result = {}
    if sys_deps:
        raise ValueError(
            "System dependencies are not supported when parsing environment.yml for "
            "the conda decorator -- use Netflix's Metaflow extensions (metaflow-netflixext)'s "
            "named environment instead"
        )

    if python_version:
        result["python"] = python_version

    if sources:
        raise ValueError(
            "Channels or extra indices are not supported when parsing environment.yml for "
            "the conda decorator -- use Netflix's Metaflow extensions (metaflow-netflixext) "
            "or specify CONDA_CHANNELS (for channels) and set indices in your pip.conf"
        )
    if pypi_deps:
        raise ValueError(
            "Mixing conda and pypi packages is not supported when parsing environment.yml for "
            "the conda decorator -- use Netflix's Metaflow extensions (metaflow-netflixext) "
            "or stick to only one ecosystem"
        )

    if len(conda_deps):
        result["libraries"] = conda_deps

    return result


def parse_req_value(
    file_content: str,
    extra_args: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    deps: Dict[str, str],
    np_deps: Dict[str, str],
    sys_deps: Dict[str, str],
) -> Optional[str]:
    from packaging.requirements import InvalidRequirement, Requirement

    python_version = None
    for line in file_content.splitlines():
        line = line.strip()
        if not line:
            continue
        splits = line.split(maxsplit=1)
        first_word = splits[0]
        if len(splits) > 1:
            rem = splits[1]
        else:
            rem = None
        if first_word in ("-i", "--index-url"):
            raise ValueError("To specify a base PYPI index, set it in your pip.conf")
        elif first_word == "--extra-index-url" and rem:
            sources.setdefault("pypi", []).append(rem)
        elif first_word in ("-f", "--find-links", "--trusted-host") and rem:
            extra_args.setdefault("pypi", []).append(" ".join([first_word, rem]))
        elif first_word in ("--pre", "--no-index"):
            extra_args.setdefault("pypi", []).append(first_word)
        elif first_word == "--conda-channel" and rem:
            sources.setdefault("conda", []).append(rem)
        elif first_word == "--conda-pkg":
            # Special extension to allow non-python conda package specification
            split_res = REQ_SPLIT_LINE.match(splits[1])
            if split_res is None:
                raise ValueError("Could not parse conda package '%s'" % splits[1])
            s = split_res.groups()
            if s[1] is None:
                np_deps[s[0].replace(" ", "")] = ""
            else:
                np_deps[s[0].replace(" ", "")] = s[1].replace(" ", "").lstrip("=")
        elif first_word == "--sys-pkg":
            # Special extension to allow the specification of system dependencies
            # (currently __cuda and __glibc)
            split_res = REQ_SPLIT_LINE.match(splits[1])
            if split_res is None:
                raise ValueError("Could not parse system package '%s'" % splits[1])
            s = split_res.groups()
            pkg_name = s[0].replace(" ", "")
            if s[1] is None:
                raise ValueError("System package '%s' requires a version" % pkg_name)
            sys_deps[pkg_name] = s[1].replace(" ", "").lstrip("=")
        elif first_word.startswith("#"):
            continue
        elif first_word.startswith("-"):
            raise ValueError(
                "'%s' is not a supported line in a requirements.txt" % line
            )
        else:
            try:
                parsed_req = Requirement(line)
            except InvalidRequirement as ex:
                raise ValueError("Could not parse '%s'" % line) from ex
            if parsed_req.marker is not None:
                raise ValueError(
                    "Environment markers are not supported for '%s'" % line
                )
            dep_name = parsed_req.name
            if parsed_req.extras:
                dep_name += "[%s]" % ",".join(parsed_req.extras)
            if parsed_req.url:
                dep_name += "@%s" % parsed_req.url
            specifier = str(parsed_req.specifier).lstrip(" =")
            if dep_name == "python":
                if specifier:
                    python_version = specifier
            else:
                deps[dep_name] = specifier
    return python_version


def parse_yml_value(
    file_content: str,
    _: Dict[str, List[str]],
    sources: Dict[str, List[str]],
    conda_deps: Dict[str, str],
    pypi_deps: Dict[str, str],
    sys_deps: Dict[str, str],
) -> Optional[str]:
    python_version = None  # type: Optional[str]
    mode = None
    for line in file_content.splitlines():
        if not line:
            continue
        elif line[0] not in (" ", "-"):
            line = line.strip()
            if line == "channels:":
                mode = "sources"
            elif line == "dependencies:":
                mode = "deps"
            elif line == "pypi-indices:":
                mode = "pypi_sources"
            else:
                mode = "ignore"
        elif mode and mode.endswith("sources"):
            line = line.lstrip(" -").rstrip()
            sources.setdefault("conda" if mode == "sources" else "pypi", []).append(
                line
            )
        elif mode and mode.endswith("deps"):
            line = line.lstrip(" -").rstrip()
            if line == "pip:":
                mode = "pypi_deps"
            elif line == "sys:":
                mode = "sys_deps"
            else:
                to_update = (
                    conda_deps
                    if mode == "deps"
                    else pypi_deps if mode == "pypi_deps" else sys_deps
                )
                splits = YML_SPLIT_LINE.split(line.replace(" ", ""), maxsplit=1)
                if len(splits) == 1:
                    if splits[0] != "python":
                        if mode == "sys_deps":
                            raise ValueError(
                                "System package '%s' requires a version" % splits[0]
                            )
                        to_update[splits[0]] = ""
                else:
                    dep_name, dep_operator, dep_version = splits
                    if dep_operator not in ("=", "=="):
                        if mode == "sys_deps":
                            raise ValueError(
                                "System package '%s' requires a specific version not '%s'"
                                % (splits[0], dep_operator + dep_version)
                            )
                        dep_version = dep_operator + dep_version
                    if dep_name == "python":
                        if dep_version:
                            if python_version:
                                raise ValueError(
                                    "Python versions specified multiple times in "
                                    "the YAML file."
                                )
                            python_version = dep_version
                    else:
                        if (
                            dep_name.startswith("/")
                            or dep_name.startswith("git+")
                            or dep_name.startswith("https://")
                            or dep_name.startswith("ssh://")
                        ):
                            # Handle the case where only the URL is specified
                            # without a package name
                            depname_and_maybe_tag = dep_name.split("/")[-1]
                            depname = depname_and_maybe_tag.split("@")[0]
                            if depname.endswith(".git"):
                                depname = depname[:-4]
                            dep_name = "%s@%s" % (depname, dep_name)

                        to_update[dep_name] = dep_version

    return python_version
