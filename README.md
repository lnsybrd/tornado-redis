Tornado Redis Client
====================

An Async redis client that utilizes the Tornado IOLoop.

Supports all commands available with redis 2.4.16.

Installing
----------

From git repo:

    sudo python setup.py install

Via pip:
    pip install redis-tornado


Usage
-----

Tests
-----

Running tests requires that a redis server be available on :6379 (default port)

Execute the runtests shell script (requires python-nose):
    
    ./runtests.sh

Or if you are so inclined, run the tests directly:

    PYTHONPATH=. && python ./tests/tests.py

