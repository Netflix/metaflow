from setuptools import setup, find_packages

version = "2.4.9"

setup(
    include_package_data=True,
    name="metaflow",
    version=version,
    description="Metaflow: More Data Science, Less Engineering",
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
        "click>=7.0",
        "requests",
        "boto3",
        "pylint",
        "importlib_metadata;python_version>='3.4' and python_version<'3.8'",
    ],
    tests_require=["coverage"],
)
