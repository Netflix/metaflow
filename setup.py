from setuptools import setup, find_packages

version = '2.2.0'

"""
Instructions on how to install ZG versions MF and KFP

To use this file to install the ZG forked version of Metaflow,
you must first install the the ZG forked version of KFP:

git clone https://github.com/alexlatchford/pipelines
cd pipelines; git checkout alexla/AIP-1676
cd sdk/python
python3 setup.py build_py

This KFP version will ensure you're able to pass in namespace and userid,
which are required for authorization reasons. Then, install this
version of Metaflow usng this `setup.py` file:

python3 setup.py install_lib

Note: if `kfp` is present in the `install_requires` list.
If you install KFP from the commands above, this setup file will locate it
and use that KFP version rather than pip installing from pypi.
"""

setup(name='metaflow',
      version=version,
      description='Metaflow: More Data Science, Less Engineering',
      author='Machine Learning Infrastructure Team at Netflix',
      author_email='help@metaflow.org',
      license='Apache License 2.0',
      packages=find_packages(exclude=['metaflow_test']),
      py_modules=['metaflow', ],
      package_data={'metaflow' : ['tutorials/*/*']},
      entry_points='''
        [console_scripts]
        metaflow=metaflow.main_cli:main
      ''',
      install_requires = [
        'click>=7.0',
        'requests',
        'boto3',
        'pylint<2.5.0',
        'kfp'
      ],
      tests_require = [
        'coverage'
      ])
