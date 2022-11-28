from setuptools import find_namespace_packages, setup


def get_long_description() -> str:
    with open("README.md") as fh:
        return fh.read()


setup(
    name="metaflow-card-via-nspackage",
    version="1.0.0",
    description="A description of your card",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your_name@yourdomain.com",
    license="Apache Software License 2.0",
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    include_package_data=True,
    zip_safe=False,
)
