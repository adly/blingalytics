from datetime import date, datetime
from decimal import Decimal
import unittest

from blingalytics import formats, sources, widgets
from blingalytics.sources import key_range
from mock import Mock

from test import reports


class TestSourceBases(unittest.TestCase):
    def setUp(self):
        self.report = reports.SuperBasicReport(Mock())

    def test_source_base(self):
        source = sources.Source(self.report)
        self.assertEqual(source.pre_process({}), None)
        self.assertEqual(list(source.get_rows([], {})), [])
        self.assertEqual(source.post_process(
            ((1,), {'a': 1, 'b': 'cat'}), {}), ((1,), {'a': 1, 'b': 'cat'}))

    def test_filter_columns(self):
        fil = sources.Filter()
        self.assertEqual(fil.columns, None)
        fil = sources.Filter(columns='col1')
        self.assertEqual(fil.columns, frozenset(['col1']))
        fil = sources.Filter(columns=['col1'])
        self.assertEqual(fil.columns, frozenset(['col1']))

    def test_footer_incrementing(self):
        col = sources.Column()
        self.assertTrue(isinstance(col.format, formats.Hidden))
        self.assertEqual(col.sort_alpha, True)
        self.assertTrue(col.footer is True)
        self.assertEqual(col.increment_footer(10, 2), 12)
        self.assertEqual(col.increment_footer(10, None), 10)
        self.assertEqual(col.increment_footer(None, 2), 2)
        self.assertEqual(col.increment_footer(None, 'string'), None)
        self.assertEqual(col.increment_footer(Decimal('1.23'), Decimal('4.56')), Decimal('5.79'))
        self.assertEqual(col.increment_footer(1.2, 3.4), 4.6)
        self.assertEqual(col.finalize_footer(12, {'othercolumn': 1, 'anothercolumn': None}), 12)
        col = sources.Column(footer=False)
        self.assertEqual(col.increment_footer(None, 2), None)
        self.assertEqual(col.increment_footer(10, 2), None)
        self.assertEqual(col.increment_footer(None, 'string'), None)

    def test_basic_key_ranges(self):
        keys = key_range.SourceKeyRange()
        self.assertEqual(keys.get_row_keys([]), [])
        keys = key_range.EpochKeyRange(datetime(2010, 1, 31), datetime(2010, 2, 1))
        self.assertEqual(list(keys.get_row_keys([])), [14640, 14641])
        keys = key_range.EpochKeyRange(date(2010, 12, 31), date(2011, 1, 2))
        self.assertEqual(list(keys.get_row_keys([])), [14974, 14975, 14976])
        keys = key_range.EpochKeyRange(date(2011, 1, 2), date(2010, 12, 31))
        self.assertRaises(ValueError, list, keys.get_row_keys([]))
        keys = key_range.EpochKeyRange('start', 'end')
        start_widget = widgets.DatePicker()
        end_widget = widgets.DatePicker()
        self.assertEqual(list(keys.get_row_keys(
            {'start': start_widget.clean('1/31/2010'), 'end': end_widget.clean('2/1/2010')})),
            [14640, 14641])
        self.assertRaises(ValueError, list, keys.get_row_keys({'othername': start_widget.clean('1/31/2010'), 'end': end_widget.clean('2/1/2010')}))

    def test_key_range_normalization(self):
        keys = sources.normalize_key_ranges(('id', key_range.SourceKeyRange))
        self.assertEqual(len(keys), 1)
        self.assertEqual(len(keys[0]), 2)
        self.assertEqual(keys[0][0], 'id')
        self.assertTrue(isinstance(keys[0][1], key_range.SourceKeyRange))
        keys = sources.normalize_key_ranges([
            ('id', key_range.SourceKeyRange),
            ('date', key_range.EpochKeyRange('start', 'end')),
        ])
        self.assertEqual(len(keys), 2)
        self.assertEqual(len(keys[0]), 2)
        self.assertEqual(len(keys[1]), 2)
        self.assertEqual(keys[0][0], 'id')
        self.assertEqual(keys[1][0], 'date')
        self.assertTrue(isinstance(keys[0][1], key_range.SourceKeyRange))
        self.assertTrue(isinstance(keys[1][1], key_range.EpochKeyRange))
