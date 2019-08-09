from builtins import map
from builtins import str
from builtins import range
from datetime import datetime
from decimal import Decimal
import unittest

import blingalytics
from blingalytics import base, caches, formats, widgets
from mock import Mock

from test import reports


class TestReportUtilities(unittest.TestCase):
    def setUp(self):
        # Ensure the report metaclass' catalog of reports is empty to start
        self._old_report_catalog = base.ReportMeta.report_catalog
        base.ReportMeta.report_catalog = []

    def tearDown(self):
        # Restore the original report catalog
        base.ReportMeta.report_catalog = self._old_report_catalog

    def test_get_report_by_code_name(self):
        # Couple reports to test with
        class StupidTestReport(base.Report):
            pass
        class NamedStupidReport(base.Report):
            code_name = 'even_more_stupid_name'

        # Nonexistent report returns None
        self.assertEqual(None,
            blingalytics.get_report_by_code_name('nonexistent_report_name'))
        # Automatic code_name returns report
        self.assertEqual(StupidTestReport,
            blingalytics.get_report_by_code_name('stupid_test_report'))
        # User-set code_name returns report
        self.assertEqual(NamedStupidReport,
            blingalytics.get_report_by_code_name('even_more_stupid_name'))

    def test_get_reports_by_category(self):
        # Couple reports to test with
        class StupidTestReport(base.Report):
            pass
        class CategorizedReport(base.Report):
            category = 'test'
        class SecondCategorizedReport(base.Report):
            category = 'test'
        class RandomCategoryReport(base.Report):
            category = 'random'

        # Ensure the right categories with the right reports
        categories = dict(blingalytics.get_reports_by_category())
        self.assertEqual(set(['test', 'random']), set(categories.keys()))
        self.assertEqual(set(categories['test']),
            set([CategorizedReport, SecondCategorizedReport]))
        self.assertEqual(categories['random'], [RandomCategoryReport])

class TestReportBase(unittest.TestCase):
    def setUp(self):
        self.mock_cache = Mock(spec=caches.Cache)
        self.report = reports.BasicDatabaseReport(self.mock_cache)

    def test_unique_ids(self):
        # Repeatable
        self.assertEqual(self.report.unique_id,
            ('basic_database_report', 'faafe977b85c59058a2a'))
        self.assertEqual(self.report.unique_id,
            ('basic_database_report', 'faafe977b85c59058a2a'))

        # Invalid user input produces error, does not change id
        self.report.clean_user_inputs(basic_database_report_user_is_active='bad')
        self.assertEqual(self.report.unique_id,
            ('basic_database_report', 'faafe977b85c59058a2a'))

        # Updating user input updates id, and is repeatable
        self.report.clean_user_inputs(basic_database_report_user_is_active='0')
        self.assertEqual(self.report.unique_id,
            ('basic_database_report', '00cc9e32a961519f4e7d'))
        self.assertEqual(self.report.unique_id,
            ('basic_database_report', '00cc9e32a961519f4e7d'))

        # Manually setting unique id overrides everything forever and ever, always
        self.report.unique_id = ('my_unique_id', '1234')
        self.assertEqual(self.report.unique_id, ('my_unique_id', '1234'))
        self.report.clean_user_inputs(basic_test_report_service='1')
        self.assertEqual(self.report.unique_id, ('my_unique_id', '1234'))

    def test_clean_user_inputs(self):
        # Starts with validation of empty user inputs
        self.assertEqual('|'.join(map(str, self.report.user_input_errors)),
            'Please choose a valid option.')
        self.assertEqual(self.report.dirty_inputs, {})
        self.assertEqual(self.report.clean_inputs, {})

        # Apply valid user inputs
        errors = self.report.clean_user_inputs(basic_database_report_user_is_active='1')
        self.assertEqual(errors, [])
        self.assertEqual(self.report.user_input_errors, [])
        self.assertEqual(self.report.dirty_inputs,
            {'basic_database_report_user_is_active': '1'})
        self.assertEqual(self.report.clean_inputs,
            {'user_is_active': True})

        # Apply invalid user inputs
        errors = self.report.clean_user_inputs(basic_database_report_user_is_active='no')
        self.assertEqual('|'.join(map(str, errors)),
            'Please choose a valid option.')
        self.assertEqual('|'.join(map(str, self.report.user_input_errors)),
            'Please choose a valid option.')
        self.assertEqual(self.report.dirty_inputs,
            {'basic_database_report_user_is_active': '1'})
        self.assertEqual(self.report.clean_inputs, {})

    def test_cache_proxy_methods(self):
        self.report.clean_user_inputs(basic_test_report_service='0')

        # Verify run_report
        self.report.run_report()
        args, kwargs = self.mock_cache.create_instance.call_args
        self.assertEqual(args[:2], ('basic_database_report', 'faafe977b85c59058a2a'))
        self.assertTrue(callable(args[2].__next__))
        self.assertEqual(args[3:], (self.report._get_footer, 1800))
        self.assertEqual(kwargs, {})

        # Verify report status methods
        self.report.is_report_started()
        self.assertEqual(self.mock_cache.is_instance_started.call_args,
            (('basic_database_report', 'faafe977b85c59058a2a'), {}))
        self.report.is_report_finished()
        self.assertEqual(self.mock_cache.is_instance_finished.call_args,
            (('basic_database_report', 'faafe977b85c59058a2a'), {}))

        # Verify cache-busting methods
        self.report.kill_cache()
        self.assertEqual(self.mock_cache.kill_instance_cache.call_args,
            (('basic_database_report', 'faafe977b85c59058a2a'), {}))
        self.report.kill_cache(full=True)
        self.assertEqual(self.mock_cache.kill_report_cache.call_args,
            (('basic_database_report',), {}))

        # Verify cached metadata methods
        self.report.report_row_count()
        self.assertEqual(self.mock_cache.instance_row_count.call_args,
            (('basic_database_report', 'faafe977b85c59058a2a'), {}))
        self.report.report_timestamp()
        self.assertEqual(self.mock_cache.instance_timestamp.call_args,
            (('basic_database_report', 'faafe977b85c59058a2a'), {}))

    def test_report_data_methods(self):
        # Verify header data
        header = self.report.report_header()
        self.assertEqual(header[0], {
            'key': '_bling_id',
            'label': 'Bling ID',
            'hidden': True,
            'sortable': False,
        })
        self.assertEqual(len(header), 6)

        # Verfiy report rows defaults
        self.mock_cache.instance_rows.return_value = []
        self.report.report_rows()
        self.assertEqual(self.mock_cache.instance_rows.call_args, (
            ('basic_database_report', 'faafe977b85c59058a2a'),
            {'sort': ('average_widget_price', 'desc'), 'selected': None, 'limit': None, 'offset': 0, 'alpha': False},
        ))
        self.report.report_rows(selected_rows=[1, 2, 3], sort=('average_widget_price', 'asc'), limit=10, offset=10)
        self.assertEqual(self.mock_cache.instance_rows.call_args, (
            ('basic_database_report', 'faafe977b85c59058a2a'),
            {'sort': ('average_widget_price', 'asc'), 'selected': [1, 2, 3], 'limit': 10, 'offset': 10, 'alpha': False},
        ))

        # Verify report rows formatting
        self.mock_cache.instance_rows.return_value = [
            {'_bling_id': 1, 'user_id': 1, 'user_is_active': True, 'num_widgets': 12, '_sum_widget_price': Decimal('25.25'), 'average_widget_price': Decimal('2.10')},
            {'_bling_id': 2, 'user_id': 3, 'user_is_active': False, 'num_widgets': 1, '_sum_widget_price': Decimal('3.50'), 'average_widget_price': Decimal('3.50')},
        ]
        rows = self.report.report_rows()
        self.assertEqual(rows, [
            [1, '1', 'Yes', '12', '25.25', '$2.10'],
            [2, '3', 'No', '1', '3.50', '$3.50'],
        ])

        # Verify footer data
        self.mock_cache.instance_footer.return_value = {
            'user_id': 3,
            'user_is_active': None,
            'num_widgets': 13,
            '_sum_widget_price': Decimal('28.75'),
            'average_widget_price': Decimal('2.21'),
        }
        footer = self.report.report_footer()
        self.assertEqual(footer, [None, '3', '', '13', '28.75', '$2.21'])

class TestFormats(unittest.TestCase):
    def test_format_base(self):
        format = formats.Format()
        self.assertEqual(format.align, 'left')
        self.assertEqual(format.sort_alpha, True)
        self.assertEqual(format.header_info, {
            'label': None,
            'sortable': True,
            'data_type': 'format',
        })
        self.assertEqual(format.format(42), '42')
        self.assertEqual(format.format_html(42), '42')
        self.assertEqual(format.format_csv(42), '42')

        format = formats.Format(label='Label', align='right')
        self.assertEqual(format.align, 'right')
        self.assertEqual(format.header_info, {
            'label': 'Label',
            'sortable': True,
            'data_type': 'format',
            'className': 'num',
        })

    def test_hidden_format(self):
        format = formats.Hidden()
        self.assertEqual(format.header_info, {
            'label': None,
            'sortable': True,
            'data_type': 'hidden',
            'hidden': True,
        })

    def test_bling_format(self):
        format = formats.Bling(label='Money Money Money!')
        self.assertEqual(format.header_info, {
            'label': 'Money Money Money!',
            'sortable': True,
            'data_type': 'bling',
            'className': 'num',
        })
        self.assertEqual(format.format_html(Decimal('12345.67')), '$12,345.67')
        self.assertEqual(format.format_csv(Decimal('12345.67')), '$12345.67')
        self.assertEqual(format.format_html(None), '$0.00')

    def test_epoch_format(self):
        format = formats.Epoch()
        self.assertEqual(format.header_info, {
            'label': None,
            'sortable': True,
            'data_type': 'epoch',
        })
        self.assertEqual(format.format_html(12), '01/13/1970')
        self.assertEqual(format.format_csv(14692), '03/24/2010')
        self.assertEqual(format.format_html(None), '')

    def test_date_format(self):
        format = formats.Date()
        self.assertEqual(format.header_info, {
            'label': None,
            'sortable': True,
            'data_type': 'date',
        })
        self.assertEqual(format.format_html(datetime(2010, 9, 22)), '09/22/2010')
        self.assertEqual(format.format_csv(datetime(1900, 12, 12)), '12/12/1900')
        self.assertEqual(format.format_csv(None), '')

    def test_integer_format(self):
        format = formats.Integer(label='Many')
        self.assertEqual(format.header_info, {
            'label': 'Many',
            'sortable': True,
            'data_type': 'integer',
            'className': 'num',
        })
        self.assertEqual(format.format_html(123456), '123,456')
        self.assertEqual(format.format_csv(123456), '123456')
        self.assertEqual(format.format_csv(None), '0')

        format = formats.Integer(label='Many', grouping=False)
        self.assertEqual(format.format_html(123456), '123456')
        self.assertEqual(format.format_csv(123456), '123456')

    def test_percent_format(self):
        format = formats.Percent(label='%')
        self.assertEqual(format.header_info, {
            'label': '%',
            'sortable': True,
            'data_type': 'percent',
            'className': 'num',
        })
        self.assertEqual(format.format_html(Decimal('12.3456')), '12.3%')
        self.assertEqual(format.format_csv(Decimal('12.3456')), '12.3%')
        self.assertEqual(format.format_html(None), '0.0%')

        format = formats.Percent(label='%', precision=0)
        self.assertEqual(format.format_html(Decimal('12.3456')), '12%')

    def test_string_format(self):
        format = formats.String(label='String-a-jobby')
        self.assertEqual(format.header_info, {
            'label': 'String-a-jobby',
            'sortable': True,
            'data_type': 'string',
        })
        self.assertEqual(format.format_html('string thing'), 'string thing')
        self.assertEqual(format.format_csv('string thing'), 'string thing')
        self.assertEqual(format.format_html(None), '')
        self.assertEqual(format.format_html(12), '12')

        format = formats.String(label='Trunc', truncate=10)
        self.assertEqual(format.format_html('Truncated text goes here'), 'Truncat...')
        format = formats.String(label='Trunc', truncate=2)
        self.assertEqual(format.format_html('Truncated text goes here'), 'Tr')

        format = formats.String(label='Title', title=True)
        self.assertEqual(format.format_html('title cased text'), 'Title Cased Text')

    def test_boolean_format(self):
        format = formats.Boolean(label='Bool')
        self.assertEqual(format.header_info, {
            'label': 'Bool',
            'sortable': True,
            'data_type': 'boolean',
        })
        self.assertEqual(format.format_html(True), 'Yes')
        self.assertEqual(format.format_csv(True), 'Yes')
        self.assertEqual(format.format_html(None), '')
        self.assertEqual(format.format_html(False), 'No')
        self.assertEqual(format.format_html(0), 'No')
        self.assertEqual(format.format_html(12), 'Yes')

        format = formats.Boolean(terms=('Uh-huh', 'Nu-uh', 'Meh'))
        self.assertEqual(format.format_html(True), 'Uh-huh')
        self.assertEqual(format.format_html(False), 'Nu-uh')
        self.assertEqual(format.format_html(None), 'Meh')

    def test_json_format(self):
        format = formats.JSON(label='Jason')
        self.assertEqual(format.header_info, {
            'label': 'Jason',
            'sortable': True,
            'data_type': 'json',
        })
        self.assertEqual(format.format_html([1, 2, 3]), '[1, 2, 3]')
        self.assertEqual(format.format_csv([1, 2, 3]), '[1, 2, 3]')

class TestWidgets(unittest.TestCase):
    def test_widget_base(self):
        # Standard functionality
        widget = widgets.Widget()
        self.assertRaises(AttributeError, lambda: widget.form_name)
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.form_name, 'report_widget')
        self.assertEqual(widget.clean(''), None)
        self.assertEqual(widget.clean(12), 12)
        self.assertRaises(NotImplementedError, widget.render)

        # With required
        widget = widgets.Widget(required=True)
        self.assertRaises(widgets.ValidationError, widget.clean, '')
        self.assertRaises(widgets.ValidationError, widget.clean, None)

    def test_checkbox_widget(self):
        widget = widgets.Checkbox(label='Bool', default=True, required=True, extra_class='bool', extra_attrs={'bool': '1'})
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Bool</label>\n  <input id="report_widget" name="report_widget" class="bl_checkbox bool" type="checkbox" checked bool="1" />')
        widget = widgets.Checkbox(label='Bool', default=lambda: False, required=False, extra_class=('one', 'two'))
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Bool</label>\n  <input id="report_widget" name="report_widget" class="bl_checkbox one two" type="checkbox"   />')
        self.assertEqual(widget.clean(''), False)
        self.assertEqual(widget.clean('1'), True)

    def test_date_picker_widget(self):
        widget = widgets.DatePicker(default='02/20/2002', extra_class=('a', 'b'), extra_attrs={'rel': 'awesome'})
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <input id="report_widget" name="report_widget" class="bl_datepicker a b" type="text" value="02/20/2002" rel="awesome" />')
        widget.default = 'today'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <input id="report_widget" name="report_widget" class="bl_datepicker a b" type="text" value="%s" rel="awesome" />' % datetime.utcnow().strftime('%m/%d/%Y'))
        widget.default = datetime.utcnow()
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <input id="report_widget" name="report_widget" class="bl_datepicker a b" type="text" value="%s" rel="awesome" />' % datetime.utcnow().strftime('%m/%d/%Y'))
        widget.default = datetime.utcnow
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <input id="report_widget" name="report_widget" class="bl_datepicker a b" type="text" value="%s" rel="awesome" />' % datetime.utcnow().strftime('%m/%d/%Y'))
        widget.date_format = '%m'
        widget.default = datetime(2002, 2, 20)
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <input id="report_widget" name="report_widget" class="bl_datepicker a b" type="text" value="02" rel="awesome" />')
        self.assertEqual(widget.clean('02'), datetime(1900, 2, 1))
        widget.date_format = '%m/%d/%Y'
        self.assertEqual(widget.clean('02/20/2002'), datetime(2002, 2, 20))
        self.assertRaises(widgets.ValidationError, widget.clean, 'crap')

    def test_select_widget(self):
        CHOICES = (
            (1, 'one'),
            (2, 'two'),
            (4, 'four'),
            (8, 'eight'),
        )
        CHOICES_CALL = lambda: [(i * i, str(i * i)) for i in range(10)]
        widget = widgets.Select(choices=CHOICES, default=0, extra_class='awesome', extra_attrs={'rel': 'super'})
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <select id="report_widget" name="report_widget" class="bl_select awesome" rel="super">'
            '<option value="0" selected>one</option><option value="1" >two</option><option value="2" >four</option><option value="3" >eight</option></select>')
        widget.default = -1
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <select id="report_widget" name="report_widget" class="bl_select awesome" rel="super">'
            '<option value="0" >one</option><option value="1" >two</option><option value="2" >four</option><option value="3" selected>eight</option></select>')
        widget.choices = CHOICES_CALL
        widget.default = lambda: 5
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <select id="report_widget" name="report_widget" class="bl_select awesome" rel="super">'
            '<option value="0" >0</option><option value="1" >1</option><option value="2" >4</option><option value="3" >9</option><option value="4" >16</option>'
            '<option value="5" selected>25</option><option value="6" >36</option><option value="7" >49</option><option value="8" >64</option><option value="9" >81</option></select>')

    def test_multiselect_widget(self):
        CHOICES_CALL = lambda: [(i * i, str(i * i)) for i in range(10)]
        widget = widgets.Multiselect(choices=CHOICES_CALL, default=0, extra_class='fail', extra_attrs={'stu': 'pendous'})
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Filter</label>\n  <select id="report_widget" name="report_widget" class="bl_multiselect fail" stu="pendous" multiple="multiple">'
            '<option value="0" selected>0</option><option value="1" >1</option><option value="2" >4</option><option value="3" >9</option><option value="4" >16</option><option value="5" >25</option>'
            '<option value="6" >36</option><option value="7" >49</option><option value="8" >64</option><option value="9" >81</option></select>')

    def test_timezone_select_widget(self):
        CHOICES = (
            (-8, 'Pacific'),
            (-5, 'Eastern'),
        )
        widget = widgets.TimezoneSelect(choices=CHOICES)
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertEqual(widget.render(),
            '<label for="report_widget">Timezone</label>\n  <select id="report_widget" name="report_widget" class="bl_select bl_timezone " ><option value="0" >Pacific</option><option value="1" >Eastern</option></select>')

    def test_autocomplete_widget(self):
        widget = widgets.Autocomplete(label='Person', default='something')
        widget._report_code_name = 'report'
        widget._name = 'widget'
        self.assertRaises(ValueError, widget.render)
        widget.default = None
        self.assertEqual(widget.render(),
            '<label for="report_widget">Person</label>\n  <input id="report_widget" name="report_widget" class="bl_autocomplete" type="text" value=""  />')
        widget.multiple = True
        self.assertEqual(widget.render(),
            '<label for="report_widget">Person</label>\n  <input id="report_widget" name="report_widget" class="bl_autocomplete bl_multiple" type="text" value=""  />')
        self.assertEqual(widget.clean(None), None)
        self.assertEqual(widget.clean(''), None)
        self.assertEqual(widget.clean('58'), [58])
        self.assertEqual(widget.clean('1234 3 99'), [1234, 3, 99])
        self.assertRaises(widgets.ValidationError, widget.clean, '123.45')
        self.assertRaises(widgets.ValidationError, widget.clean, '123 45 x')
        widget.multiple = False
        self.assertRaises(widgets.ValidationError, widget.clean, '1234 3 99')
