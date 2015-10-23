#!/usr/bin/env python

from setuptools import setup 
import os

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()

setup(
    name="reporting-pollster",
    version="0.1.0",
    author="NCI Cloud Team",
    author_email="cloud.team@nci.org.au",
    url="https://github.com/NCI-Coud/reporting-pollster",
    license="Apache 2.0",
    description="OpenStack reporting pollster system",
    long_description=(read("README.md")),
    packages=['common', 'entities'],
    scripts=['update-db.py'],
    data_files=[
        ('/etc/reporting-pollster', ['files/reporting.conf']),
    ],
    install_requires=open('REQUIREMENTS.txt').read().splitlines()
)

