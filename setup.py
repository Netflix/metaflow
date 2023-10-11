from setuptools import setup, find_packages

version = "2.10.2"

setup(
    include_package_data=True,
    name="metaflow",
    version=version,
    description="Metaflow: More Data Science, Less Engineering",
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
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
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
    package_data={"metaflow": ["tutorials/*/*", "py.typed"]},
    entry_points="""
        [console_scripts]
        metaflow=metaflow.cmd.main_cli:start
      """,
    install_requires=[
        "requests",
        "boto3",
    ],
)
