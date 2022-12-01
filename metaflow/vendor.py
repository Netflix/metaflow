import glob
import shutil
import subprocess
import re

from functools import partial
from itertools import chain
from pathlib import Path

WHITELIST = {
    "README.txt",
    "__init__.py",
    "vendor_any.txt",
    "vendor_v3_5.txt",
    "vendor_v3_6.txt",
    "pip.LICENSE",
}

# Borrowed from https://github.com/pypa/pip/tree/main/src/pip/_vendor

VENDOR_SUBDIR = re.compile(r"^_vendor/vendor_([a-zA-Z0-9_]+).txt$")


def delete_all(*paths, whitelist=frozenset()):
    for item in paths:
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        elif item.is_file() and item.name not in whitelist:
            item.unlink()


def iter_subtree(path):
    """Recursively yield all files in a subtree, depth-first"""
    if not path.is_dir():
        if path.is_file():
            yield path
        return
    for item in path.iterdir():
        if item.is_dir():
            yield from iter_subtree(item)
        elif item.is_file():
            yield item


def patch_vendor_imports(file, replacements):
    text = file.read_text("utf8")
    for replacement in replacements:
        text = replacement(text)
    file.write_text(text, "utf8")


def find_vendored_libs(vendor_dir, whitelist, whitelist_dirs):
    vendored_libs = []
    paths = []
    for item in vendor_dir.iterdir():
        if item.is_dir() and item not in whitelist_dirs:
            vendored_libs.append(item.name)
        elif item.is_file() and item.name not in whitelist:
            vendored_libs.append(item.stem)  # without extension
        else:  # not a dir or a file not in the whitelist
            continue
        paths.append(item)
    return vendored_libs, paths


def fetch_licenses(*info_dir, vendor_dir):
    for file in chain.from_iterable(map(iter_subtree, info_dir)):
        if "LICENSE" in file.name:
            library = file.parent.name.split("-")[0]
            shutil.copy(file, vendor_dir / ("%s.LICENSE" % library))
        else:
            continue


def vendor(vendor_dir):
    # remove everything
    delete_all(*vendor_dir.iterdir(), whitelist=WHITELIST)

    exclude_subdirs = []
    # Iterate on the vendor*.txt files
    for vendor_file in glob.glob(f"{vendor_dir.name}/vendor*.txt"):
        # We extract the subdirectory we are going to extract into
        subdir = VENDOR_SUBDIR.match(vendor_file).group(1)
        # Includes "any" but it doesn't really matter unless you install "any"
        exclude_subdirs.append(subdir)

    for subdir in exclude_subdirs:
        create_init_file = False
        if subdir == "any":
            vendor_subdir = vendor_dir
            # target package is <parent>.<vendor_dir>; foo/_vendor -> foo._vendor
            pkgname = f"{vendor_dir.parent.name}.{vendor_dir.name}"
        else:
            create_init_file = True
            vendor_subdir = vendor_dir / subdir
            # target package is <parent>.<vendor_dir>; foo/_vendor -> foo._vendor
            pkgname = f"{vendor_dir.parent.name}.{vendor_dir.name}.{vendor_subdir.name}"

        # install with pip
        subprocess.run(
            [
                "python3",
                "-m",
                "pip",
                "install",
                "-t",
                str(vendor_subdir),
                "-r",
                "_vendor/vendor_%s.txt" % subdir,
                "--no-compile",
            ]
        )

        # fetch licenses
        fetch_licenses(*vendor_subdir.glob("*.dist-info"), vendor_dir=vendor_subdir)

        # delete stuff that's not needed
        delete_all(
            *vendor_subdir.glob("*.dist-info"),
            *vendor_subdir.glob("*.egg-info"),
            vendor_subdir / "bin",
        )

        # Touch a __init__.py file
        if create_init_file:
            with open(
                "%s/__init__.py" % str(vendor_subdir), "w+", encoding="utf-8"
            ) as f:
                f.write("# Empty file")

        vendored_libs, paths = find_vendored_libs(
            vendor_subdir, WHITELIST, exclude_subdirs
        )

        replacements = []
        for lib in vendored_libs:
            replacements += (
                partial(  # import bar -> import foo._vendor.bar
                    re.compile(r"(^\s*)import {}\n".format(lib), flags=re.M).sub,
                    r"\1from {} import {}\n".format(pkgname, lib),
                ),
                partial(  # from bar -> from foo._vendor.bar
                    re.compile(r"(^\s*)from {}(\.|\s+)".format(lib), flags=re.M).sub,
                    r"\1from {}.{}\2".format(pkgname, lib),
                ),
            )

        for file in chain.from_iterable(map(iter_subtree, paths)):
            if file.suffix == ".py":
                patch_vendor_imports(file, replacements)


if __name__ == "__main__":
    here = Path("__file__").resolve().parent
    vendor_tl_dir = here / "_vendor"
    has_vendor_file = len(glob.glob(f"{vendor_tl_dir.name}/vendor*.txt")) > 0
    assert has_vendor_file, "_vendor/vendor*.txt file not found"
    assert (
        vendor_tl_dir / "__init__.py"
    ).exists(), "_vendor/__init__.py file not found"
    vendor(vendor_tl_dir)
