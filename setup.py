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
    py_modules=[
        "metaflow",
    ],
    package_data={"metaflow": ["tutorials/*/*"]},
    entry_points="""
        [console_scripts]
        metaflow=metaflow.main_cli:main
      """,
    install_requires=[
        "requests",
        "boto3",
        "pylint",
    ],
    extras_require={
        "kfp": ["zillow-kfp", "kfp-server-api"],
        # Use an extras here as there is no "extras_tests_require" functionality :(
        "kfp-tests": ["pytest", "pytest-xdist", "pytest-cov", "subprocess-tee"],
    },
)
