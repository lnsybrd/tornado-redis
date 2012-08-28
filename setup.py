#!/usr/bin/python2

from setuptools import setup

setup(name='tornado-redis',
    version='0.2',
    description='Redis client built on the Tornado IOLoop',
    author='Lin Salisbury',
    author_email='lin.salisbury@gmail.com',
    url='http://www.tweezercode.com',
    packages=['redis'],
    install_requires=['tornado'])
