"""
For now, run the tests from the blingalytics containing directory with:

PYTHONPATH=. python tests/test_main.py
"""

from datetime import date
import unittest

from blingalytics import base, formats, sources
from blingalytics.caches import local, redis_cache
from blingalytics.sources import static


class SuperBasicReport(base.Report):
    filters = []
    keys = ('id', sources.EpochKeyRange(date(2011, 1, 1), date(2011, 1, 3)))
    columns = [
        ('id', static.Value(1, format=formats.Integer)),
    ]
    default_sort = ('id', 'desc')

class BasicTest(unittest.TestCase):
    def test_basic_local(self):
        report = SuperBasicReport(local.LocalCache())
        report.kill_cache(full=True)
        report.run_report()
        rows = report.report_rows()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][1], '1')

    def test_basic_redis(self):
        report = SuperBasicReport(redis_cache.RedisCache())
        report.kill_cache(full=True)
        report.run_report()
        rows = report.report_rows()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][1], '1')

if __name__ == '__main__':
    import sys
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BasicTest))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(len(result.errors) + len(result.failures))
