from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.index import PackageFinder

version = '0.1.0'

finder = PackageFinder(find_links=[],
                       session=False,
                       index_urls=["http://pypi.python.org/simple/"])
requirements = parse_requirements('requirements.txt',
                                  session=False,
                                  finder=finder)

setup(name='reporting-pollster',
      version=version,
      description='OpenStack reporting pollster system',
      author='NeCTAR',
      author_email='',
      url='https://github.com/NeCTAR-RC/reporting-pollster',
      license='Apache 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      install_requires=[str(r.req) for r in requirements],
      scripts=['reporting-pollster'],
      test_suite="tests.test_all"
)
