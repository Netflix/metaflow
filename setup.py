from setuptools import setup, find_packages

version = '2.3.2'

setup(name='zillow-metaflow',
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
        'click>=7.0,<8',
        'requests',
        'boto3',
        'pylint'
      ],
      tests_require = [
        'coverage'
      ],
      extras_require = {
        'kfp': 'zillow-kfp'
      })
