from decimal import Decimal
import unittest

from blingalytics import widgets
from blingalytics.sources import merge
from mock import Mock

from test import entities, reports


class TestMergeSource(unittest.TestCase):
    def setUp(self):
        cache = Mock()
        cache.instance_rows.return_value = [
            {'_bling_id': 1, 'user_id': 1, 'user_is_active': True, 'num_widgets': 2, '_sum_widget_price': Decimal('6.00'), 'average_widget_price': Decimal('3.00')},
            {'_bling_id': 2, 'user_id': 2, 'user_is_active': False, 'num_widgets': 10, '_sum_widget_price': Decimal('100.00'), 'average_widget_price': Decimal('10.00')},
        ]
        self.report = reports.BasicMergeReport(cache)

    def test_merge_source(self):
        id1, id2 = entities.Compare(), entities.Compare()
        self.report.clean_user_inputs(include='1', user_is_active='0')
        source = merge.MergeSource(self.report)
        self.assertEqual(list(source.get_rows([], self.report.clean_inputs)), [
            ((id1,), {'double_num_widgets': 4, 'user_id': 1, 'user_is_active': True}),
            ((id2,), {'double_num_widgets': 20, 'user_id': 2, 'user_is_active': False}),
        ])
        self.report.clean_user_inputs(include='', user_is_active='0')
        self.assertEqual(list(source.get_rows([], self.report.clean_inputs)), [
            ((id1,), {'double_num_widgets': 2, 'user_id': 1, 'user_is_active': True}),
            ((id2,), {'double_num_widgets': 10, 'user_id': 2, 'user_is_active': False}),
        ])

    def test_merge_columns(self):
        # Test basic merge column functionality, and Sum functionality
        col = merge.Sum() # Should merge any columns with the column name given (second arg)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 4)
        self.assertEqual(col._merge_report_column('report1', 'col3', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), None)
        self.assertEqual(col._merge_report_column('report23883832', 'col2', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 6)
        self.assertEqual(col._merge_report_column('report1', 'col1', {}, {'col1': 3, 'col2': 4}), 3)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': Decimal('1.45'), 'col2': 2}, {'col1': Decimal('2.01'), 'col2': 4}), Decimal('3.46'))
        self.assertRaises(TypeError, col._merge_report_column, 'report1', 'col1', {'col1': 'string', 'col2': 2}, {'col1': Decimal('1.5'), 'col2': 4})
        col = merge.Sum('col1') # Should merge col1 columns regardless of the second arg
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 4)
        self.assertEqual(col._merge_report_column('report1', 'col3', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 3)
        self.assertEqual(col._merge_report_column('report23883832', 'col1', {'col1': 1, 'col2': 2}, {'col2': 4}), 1)
        self.assertEqual(col._merge_report_column('report1', 'col2', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 5)
        col = merge.Sum('report1.col1') # Should merge only col1 from report1
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 4)
        self.assertEqual(col._merge_report_column('report1', 'col2', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 5)
        self.assertEqual(col._merge_report_column('report2', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 1)
        self.assertEqual(col._merge_report_column('report2', 'col2', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 2)
        col = merge.Sum('report1.col1', 'report2.col2') # Should merge col1 from report1, col2 from report2
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 4)
        self.assertEqual(col._merge_report_column('report1', 'col2', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 5)
        self.assertEqual(col._merge_report_column('report2', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 5)
        self.assertEqual(col._merge_report_column('report2', 'col2', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 6)
        self.assertEqual(col._merge_report_column('report3', 'col3', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), None)

        # First
        col = merge.First()
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}), 1)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': 3, 'col2': 4}), 3)
        self.assertEqual(col._merge_report_column('report1', 'col1', {}, {'col1': 3, 'col2': 4}), 3)

        # BoolAnd
        col = merge.BoolAnd()
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': True, 'col2': 4}), True)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': True, 'col2': 2}, {'col1': None, 'col2': 4}), True)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': False, 'col2': 4}), False)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': True, 'col2': 2}, {'col1': False, 'col2': 4}), False)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': False, 'col2': 2}, {'col1': False, 'col2': 4}), False)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': None, 'col2': 4}), True)

        # BoolOr
        col = merge.BoolOr()
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': True, 'col2': 4}), True)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': True, 'col2': 2}, {'col1': None, 'col2': 4}), True)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': False, 'col2': 4}), False)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': True, 'col2': 2}, {'col1': False, 'col2': 4}), True)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': False, 'col2': 2}, {'col1': False, 'col2': 4}), False)
        self.assertEqual(col._merge_report_column('report1', 'col1', {'col1': None, 'col2': 2}, {'col1': None, 'col2': 4}), False)

    def test_merge_filters(self):
        # PostFilter
        fil = merge.PostFilter(lambda row: row['include'] in ('yes', 'please'))
        self.assertEqual(fil.include_row({'include': 'yes', 'value': 2}, {}), True)
        self.assertEqual(fil.include_row({'include': 'maybe so', 'value': 2}, {}), False)
        self.assertRaises(KeyError, fil.include_row, {'value': 1, 'othervalue': 2}, {})
        widget = widgets.Select(choices=((True, 'Include'), (False, 'Disclude')))
        widget._name = 'widget'
        fil = merge.PostFilter(lambda row, user_input: user_input, widget=widget)
        self.assertEqual(fil.include_row({'value': 1}, {'widget': widget.clean(1)}), False)

        # ReportFilter
        widget = widgets.Checkbox()
        widget._name = 'widget'
        fil = merge.ReportFilter('report1', widget=widget)
        self.assertEqual(fil.excluded_reports({'widget': widget.clean(True)}), [])
        self.assertEqual(fil.excluded_reports({'widget': widget.clean(False)}), ['report1'])

        # DelegatedFilter
        del1 = Mock()
        del2 = Mock()
        fil = merge.DelegatedFilter(del1)
