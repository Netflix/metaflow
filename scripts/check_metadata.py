import configparser
import re
import sys


def read_setup_cfg():
    config = configparser.ConfigParser()
    config.read("setup.cfg")

    return {
        "name": config.get("metadata", "name", fallback=None),
        "version": config.get("metadata", "version", fallback=None),
    }


def read_setup_py():
    with open("setup.py", "r") as f:
        content = f.read()

    name_match = re.search(r"name\s*=\s*['\"](.+?)['\"]", content)

    return {
        "name": name_match.group(1) if name_match else None,
    }


def read_version_file():
    with open("metaflow/version.py", "r") as f:
        line = f.read().splitlines()[0]

    version = line.split("=")[1].strip(" \"'")
    return version


def main():
    cfg = read_setup_cfg()
    py = read_setup_py()
    version = read_version_file()

    errors = []
    checks_performed = 0

    # Check name ONLY if present in both
    if cfg["name"] is not None and py["name"] is not None:
        checks_performed += 1
        if cfg["name"] != py["name"]:
            errors.append(
                f"Name mismatch: setup.cfg='{cfg['name']}' vs setup.py='{py['name']}'"
            )

    # Check version ONLY if present in cfg
    if cfg["version"] is not None:
        checks_performed += 1
        if cfg["version"] != version:
            errors.append(
                f"Version mismatch: setup.cfg='{cfg['version']}' vs version.py='{version}'"
            )

    # If nothing to check → warn instead of false pass
    if checks_performed == 0:
        print("⚠️ No overlapping metadata fields found to validate.")
        sys.exit(0)

    if errors:
        print("❌ Metadata consistency check FAILED:\n")
        for err in errors:
            print(f"- {err}")
        sys.exit(1)

    print("✅ Metadata consistency check PASSED")

if __name__ == "__main__":
    main()
