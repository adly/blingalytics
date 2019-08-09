from __future__ import division
from past.utils import old_div
from decimal import Decimal
import unittest

from blingalytics.sources import derived
from mock import Mock

from test import reports


class TestDerivedSource(unittest.TestCase):
    def setUp(self):
        self.report = reports.BasicDatabaseReport(Mock())

    def test_derived_source(self):
        source = derived.DerivedSource(self.report)
        self.assertEqual(len(source._columns), 1)
        self.assertEqual(len(source._columns[0]), 2)
        self.assertEqual(source._columns[0][0], 'average_widget_price')
        self.assertEqual(list(source._columns_dict), ['average_widget_price'])
        self.assertTrue(isinstance(source._columns[0][1], derived.Value))
        self.assertEqual(source.pre_process({}), None)
        self.assertEqual(list(source.get_rows([], {})), [])
        self.assertEqual(
            source.post_process({'num_widgets': 2, '_sum_widget_price': Decimal('15.00'), 'othercolumn': 'string'}, {}),
            {'num_widgets': 2, '_sum_widget_price': Decimal('15.00'), 'othercolumn': 'string', 'average_widget_price': Decimal('7.50')})

    def test_derived_column(self):
        col = derived.Value(lambda row: old_div(row['x'], row['y']))
        self.assertEqual(col.get_derived_value({'x': Decimal('5.0'), 'y': Decimal('10.0')}), Decimal('0.5'))
        self.assertEqual(col.get_derived_value({'x': None, 'y': Decimal('10.0')}), None)
        self.assertEqual(col.get_derived_value({'x': Decimal('5.0'), 'y': Decimal('0.0')}), Decimal('0.00'))
        self.assertEqual(col.get_derived_value({'x': 2, 'y': 0}), Decimal('0.00'))
        self.assertEqual(col.increment_footer(None, Decimal('1.2')), None)
        self.assertEqual(col.increment_footer(None, None), None)
        self.assertEqual(col.finalize_footer(None, {'x': Decimal('20.5'), 'y': Decimal('0.5'), 'othervalue': 'string'}), Decimal('41.0'))
        self.assertEqual(col.finalize_footer(None, {'x': Decimal('20.5'), 'y': Decimal('0.0'), 'othervalue': 'string'}), Decimal('0.00'))
        self.assertEqual(col.finalize_footer(None, {'x': Decimal('20.5'), 'y': None, 'othervalue': 'string'}), None)
