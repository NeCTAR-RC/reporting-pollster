try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements
from setuptools import find_packages
from setuptools import setup

version = '0.1.0'

requirements = parse_requirements("requirements.txt", session=False)

setup(name='reporting-pollster',
      version=version,
      description='OpenStack reporting pollster system',
      author='NeCTAR',
      author_email='',
      url='https://github.com/NeCTAR-RC/reporting-pollster',
      license='Apache 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      install_requires=[str(r.req) for r in requirements],
      scripts=['reporting-pollster', 'reporting-db-sync'],
      test_suite="tests.test_all"
      )
