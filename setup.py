#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setup(
    name="relations-pymysql",
    version="0.6.11",
    package_dir = {'': 'lib'},
    py_modules = [
        'relations_pymysql'
    ],
    install_requires=[
        'PyMySQL==0.10.0',
        'relations-mysql>=0.6.2'
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
