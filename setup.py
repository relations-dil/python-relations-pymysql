#!/usr/bin/env python

from setuptools import setup, find_packages
setup(
    name="relations-pymysql",
    version="0.1.0",
    package_dir = {'': 'lib'},
    py_modules = [
        'relations_pymysql'
    ],
    install_requires=[
        'PyMySQL==0.10.0'
    ]
)
