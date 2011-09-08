"""
The test suite has some dependencies that aren't necessarily required for the
blingalytics package itself:

* You should have postgresql installed, with a "bling" user whose password is
  set to "bling", and a database named "bling" owned by "bling". 
* You need the following Python packages installed: mock, sqlalchemy, elixir,
  and psycopg2.

To run the tests, simply run this file::

    python test_runner.py

"""

import decimal
import locale
import os
import sys
import unittest


# Set standard thread-wide locale and decimal rounding settings
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
decimal.setcontext(decimal.Context(rounding=decimal.ROUND_HALF_UP))

if __name__ == '__main__':
    test_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.normpath(os.path.join(test_dir, os.pardir, 'blingalytics'))
    sys.path = [test_dir, package_dir] + sys.path

    suite = unittest.TestLoader().loadTestsFromNames([
        'test_base',
        'test_helpers',
        'sources.test_base',
        'sources.test_database',
        'sources.test_derived',
        'sources.test_merge',
        'sources.test_static',
    ])
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    sys.exit(len(result.errors) + len(result.failures))
