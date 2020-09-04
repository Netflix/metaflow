from setuptools import setup, find_packages

version = '2.2.2'

"""
To use this version of Metaflow (which includes KFP integration), you
must first install the compatible version of KFP:

  - git clone -b alexla/AIP-1676 https://github.com/alexlatchford/pipelines
  - cd pipelines/sdk/python
  - pip3 install -e .
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
