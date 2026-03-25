import os, glob
from setuptools import setup, find_packages

with open("metaflow/version.py", mode="r") as f:
    version = f.read().splitlines()[0].split("=")[1].strip(" \"'")


def find_devtools_files():
    # Returns a list of (install_dir, [files]) tuples preserving subdirectory structure.
    entries = {}
    for path in glob.iglob("devtools/**/*", recursive=True):
        if os.path.isfile(path):
            rel_dir = os.path.dirname(path)  # e.g. "devtools/tilt/k8s"
            install_dir = os.path.join(
                "share/metaflow", rel_dir
            )  # e.g. "share/metaflow/devtools/tilt/k8s"
            entries.setdefault(install_dir, []).append(path)
    return list(entries.items())


setup(
    include_package_data=True,
    name="metaflow",
    version=version,
    description="Metaflow: More AI and ML, Less Engineering",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Metaflow Developers",
    author_email="help@metaflow.org",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    project_urls={
        "Source": "https://github.com/Netflix/metaflow",
        "Issues": "https://github.com/Netflix/metaflow/issues",
        "Documentation": "https://docs.metaflow.org",
    },
    packages=find_packages(exclude=["metaflow_test"]),
    py_modules=[
        "metaflow",
    ],
    package_data={
        "metaflow": [
            "tutorials/*/*",
            "plugins/env_escape/configurations/*/*",
            "py.typed",
            "**/*.pyi",
        ]
    },
    data_files=find_devtools_files(),
    entry_points="""
        [console_scripts]
        metaflow=metaflow.cmd.main_cli:start
        metaflow-dev=metaflow.cmd.make_wrapper:main
      """,
    install_requires=["requests", "boto3"],
    extras_require={
        "stubs": ["metaflow-stubs==%s" % version],
        "dev": [
            "pytest",
            "pytest-timeout",
            "pytest-cov",
            "pytest-xdist",
            "pytest-rerunfailures",
            "omegaconf",
            "kubernetes",
            "localbatch; python_version >= '3.8'",
        ],
    },
)
