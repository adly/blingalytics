"""
For now, run the tests from the blingalytics containing directory with:

PYTHONPATH=. python tests/test_main.py
"""

import decimal
import locale
import unittest

from blingalytics.caches import local, redis_cache

from test import test_entities as entities, test_reports as reports


# Set standard thread-wide locale and decimal rounding settings
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
decimal.setcontext(decimal.Context(rounding=decimal.ROUND_HALF_UP))

class BasicTest(unittest.TestCase):
    def setUp(self):
        entities.init_db_from_scratch()

    def test_basic_local(self):
        report = reports.SuperBasicReport(local.LocalCache())
        report.kill_cache(full=True)
        report.run_report()
        rows = report.report_rows()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][1], '1')

    def test_basic_redis(self):
        report = reports.SuperBasicReport(redis_cache.RedisCache())
        report.kill_cache(full=True)
        report.run_report()
        rows = report.report_rows()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][1], '1')

    def test_database_source(self):
        report = reports.BasicDatabaseReport(redis_cache.RedisCache())
        report.kill_cache(full=True)
        report.run_report()
        rows = report.report_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1:], ['2', 'No', '1', '50.00', '$50.00'])
        self.assertEqual(rows[1][1:], ['1', 'Yes', '3', '7.02', '$2.34'])

if __name__ == '__main__':
    import sys
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BasicTest))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(len(result.errors) + len(result.failures))
