import unittest

from blingalytics.sources import static
from mock import Mock

from test import reports


class TestStaticSource(unittest.TestCase):
    def setUp(self):
        self.report = reports.SuperBasicReport(Mock())

    def test_static_source(self):
        source = static.StaticSource(self.report)
        self.assertEqual(len(source._columns), 1)
        self.assertEqual(len(source._columns[0]), 2)
        self.assertEqual(source._columns[0][0], 'id')
        self.assertEqual(list(source._columns_dict), ['id'])
        self.assertTrue(isinstance(source._columns[0][1], static.Value))
        self.assertEqual(source.pre_process({}), None)
        self.assertEqual(list(source.get_rows([], {})), [])
        self.assertEqual(source.post_process({'othercolumn': 'stuff'}, {}),
            {'othercolumn': 'stuff', 'id': 1})
