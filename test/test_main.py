"""
For now, run the tests from the blingalytics containing directory with:

python tests/test_main.py
"""

import decimal
import locale
import unittest


# Set standard thread-wide locale and decimal rounding settings
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
decimal.setcontext(decimal.Context(rounding=decimal.ROUND_HALF_UP))

if __name__ == '__main__':
    import sys

    suite = unittest.TestLoader().loadTestsFromNames([
        'test.test_main',
        'test.test_base',
        'test.sources.test_base',
        'test.sources.test_database',
        'test.sources.test_derived',
        'test.sources.test_merge',
        'test.sources.test_static',
    ])
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    sys.exit(len(result.errors) + len(result.failures))
