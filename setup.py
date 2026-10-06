#!/usr/bin/env python

import os
from setuptools import setup

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

version = os.environ.get("BUILD_VERSION")

if version is None:
    with open("VERSION", "r") as version_file:
        version = version_file.read().strip()

setup(
    name="relations-pymysql",
    version=version,
    package_dir = {'': 'lib'},
    py_modules = [
        'relations_pymysql'
    ],
    install_requires=[
        'PyMySQL==0.10.0',
        'relations-dil>=0.6.14',
        'relations-mysql>=0.6.4'
    ],
    url="https://github.com/relations-dil/python-relations-pymysql",
    author="Gaffer Fitch",
    author_email="relations@gaf3.com",
    description="DB Modeling for MySQL using the PyMySQL library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license_files=('LICENSE.txt',),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ]
)
