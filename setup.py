from setuptools import setup, find_packages

version = "2.5.4"

setup(
    include_package_data=True,
    name="zillow-metaflow",
    version=version,
    description="Metaflow: More Data Science, Less Engineering",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Machine Learning Infrastructure Team at Netflix",
    author_email="help@metaflow.org",
    license="Apache License 2.0",
    packages=find_packages(exclude=["metaflow_test"]),
    py_modules=["metaflow", "kfp"],
    package_data={"metaflow": ["tutorials/*/*"]},
    entry_points="""
        [console_scripts]
        metaflow=metaflow.main_cli:main
      """,
    install_requires=[
        "requests",
        "boto3",
        "pylint",
        # required for KFP
        "absl-py>=0.9,<=0.11",
        "Deprecated>=1.2.7,<2",
        "docstring-parser>=0.7.3,<1",
        "fire>=0.3.1,<1",
        "googleapis-common-protos>=1.6.0,<2",
        "jsonschema>=4.19.2,<5",
        "kfp-pipeline-spec>=0.1.13,<0.2.0",
        "kfp-server-api>=1.1.2,<2.0.0",
        "kubernetes>=8.0.0,<27",
        "protobuf>=3.13.0,<4",
        "pyyaml>=5.3,<7",
        # AIP-8457(talebz): WFSDK requests-toolbelt dependency breaks KFNB "pip install poetry"
        # "requests-toolbelt>=0.8.0,<1",
        # "uritemplate>=3.0.1,<4",
    ],
    tests_require=["coverage"],
    extras_require={
        # Use an extras here as there is no "extras_tests_require" functionality :(
        "aip-tests": ["pytest", "pytest-xdist", "pytest-cov", "subprocess-tee"],
    },
)
