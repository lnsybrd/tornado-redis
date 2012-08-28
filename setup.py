from setuptools import setup, find_packages

setup(
    name = 'redis-tornado',
    version = '0.2',
    description = 'Async redis client built on the Tornado IOLoop.',

    author = 'Lin Salisbury',
    author_email = 'lin.salisbury@gmail.com',

    url = 'http://www.tweezercode.com',

    license = 'APACHE',

    packages = find_packages(),

    install_requires = [
        'tornado >= 2.2.1'
    ],
)
