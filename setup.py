import os

from setuptools import setup, find_packages

version = '2.3.2+zg1.1'

# TODO: once this branch is merged or in pip use, remove this
os.system(
    "pip3 install 'git+https://github.com/zillow/pipelines@feature/zg#egg=kfp&subdirectory=sdk/python'"
)

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
        'kfp',
        'pylint',
      ],
      tests_require = [
        'coverage'
      ])
