"""
Blingalytics Reporting Infrastructure.

The blingalytics project provides a mechanism to run reports over large
datasets, process the data, format the data, and return tabular results.

For more information on using and writing your own sources, key_ranges,
formats, and widgets, see the documentation in those modules. For an
explanation of how to write a report, see the documentation on the Report
class below.
"""

import copy
import hashlib
import heapq
import itertools
import re

from blingalytics import sources, widgets


DEFAULT_CACHE_TIME = 60 * 30

def get_display_name(class_name):
    """
    Converts class names to a title case style.
    
    For example, 'CelebrityApprovalReport' would become 'Celebrity Approval
    Report'.
    """
    # Thanks to Django's code for a portion of this regex
    display = re.sub('(((?<=[a-z])[A-Z])|([A-Z](?![A-Z]|$)))', ' \\1', class_name)
    display = display.replace('_', ' ').title()
    return display.strip()

def get_code_name(class_name):
    """
    Converts class names to a Pythonic code name style.
    
    For example, 'CelebrityApprovalReport' would be converted to
    'celebrity_approval_report'.
    """
    return get_display_name(class_name).replace(' ', '_').lower()

class ReportMeta(type):
    report_catalog = []

    def __new__(cls, name, bases, dct):
        # Ensure the report class has a display name and code name
        dct['display_name'] = dct.get('display_name', get_display_name(name))
        dct['code_name'] = dct.get('code_name', get_code_name(name))

        # Ensure each report class has its own copy of its filters/widgets
        # (in case a widget instance is saved and used in many reports)
        report_filters = []
        report_widgets = []
        for fil_name, fil in dct.get('filters', []):
            filter_copy = copy.copy(fil)
            if filter_copy.widget:
                widget_copy = copy.copy(filter_copy.widget)
                widget_copy._name = fil_name
                widget_copy._report_code_name = dct['code_name']
                filter_copy.widget = widget_copy
                report_widgets.append((fil_name, widget_copy))
            report_filters.append((fil_name, filter_copy))
        dct['filters'] = report_filters
        dct['widgets'] = report_widgets

        # Keep a list of all the reports that exist
        report_cls = type.__new__(cls, name, bases, dct)
        cls.report_catalog.append(report_cls)

        return report_cls

class Report(object):
    """
    Base class for report definitions.
    
    To define a new report, you create a subclass of this Report class and
    set various options as attributes on your class. The available attributes
    are:
    
    * category: A string representing the category this report belongs to. The
      category is used by the get_reports_by_category function to group like
      reports together. Optional.
    * display_name: The title of the report as a string. Optional, will be
      automatically generated from the class name.
    * code_name: The codename of the report as a string. Optional, will be
      automatically generated from the class name.
    * cache_time: The number of seconds this report should remain in cache
      before expiring. Optional, defaults to 30 minutes.
    * keys: If you have just one key on your report, this should be a
      two-tuple of the string column name and a key_range class or instance.
      If you want do define a report with compound keys, this can be a list of
      such two-tuples. The key range for your report basically defines the
      granularity of the rows in the output, and is described in more detail
      in the key_ranges documentation.
    * columns: This should be a list of two-tuples, with the first element
      a string code name of the column, and the second being a column class or
      instance. These columns define what data is getting pulled from your
      data sources, how it is calculated, and how it is formatted. For more
      on these topics, see the documentation in sources and formats.
    * filters: A list of two-tuples, with the first element being a string
      name for the filter, and the second being the filter and associated
      widget. For more detail on filters, see the sources and widgets
      documentation.
    * default_sort: A two-tuple representing the column and direction by which
      the report should be sorted by default. The first value is the column
      name and the second is a string, either 'asc' or 'desc'. Optional,
      defaults to sorting by the first report column descending.

    Other attributes may also be allowed or required by the specific sources
    utilized by your report. For details, see those sources' documentation.
    
    Here is a short example report definition:
    
    from logic.blingalytics3 import base, formats, key_ranges, widgets
    from logic.blingalytics3.sources import database, derived
    
    class RevenueReport(base.Report):
        category = 'revenue'
        database_entity = 'models.reporting.RevenueModel'
        cache_time = 60 * 60 * 3 # three hours
        
        filters = [
            database.QueryFilter(lambda entity: entity.is_delivered == True),
            database.QueryFilter(
                lambda entity, user_input: entity.service == user_input \
                if user_input else None,
                widget=widgets.Select(label='Service', choices=SERVICES)),
        ]
        keys = ('contract_id', key_ranges.SourceKeyRange)
        columns = [
            ('contract_id', database.GroupBy('contract_id',
                format=formats.Integer(label='ID', grouping=False),
                footer=False)),
            ('contract_name', database.Lookup('model.contracts.Contract',
                'name', 'contract_id', format=formats.String)),
            ('revenue', database.Sum('contract_revenue',
                format=formats.Bling)),
            ('_net_revenue', database.Sum('contract_net_revenue')),
            ('gross_margin', derived.Value(
                lambda row: row['_net_revenue'] / row['revenue'] * Decimal('100.00'),
                format=formats.Bling)),
        ]
        default_sort = ('gross_margin', 'desc')
    """
    __metaclass__ = ReportMeta

    def __init__(self, cache, merge=False):
        self.cache = cache
        
        # Grab an instance of each of the source types implied by the columns
        self.columns_dict = dict(self.columns)
        report_sources = set([c.source for c in self.columns_dict.values()])
        self._sources = [source(self) for source in report_sources]

        # Set default format labels
        for name, column in self.columns:
            if column.format.label is None:
                column.format.label = get_display_name(name)

        # Some defaults for optional settings
        self._init_footer()
        self.keys = sources.normalize_key_ranges(self.keys)
        self.cache_time = getattr(self, 'cache_time', DEFAULT_CACHE_TIME)
        self.default_sort = getattr(self, 'default_sort',
            (self.columns[0][0], 'desc'))
        self.dirty_inputs = {}
        self.clean_inputs = {}
        if not merge:
            self.clean_user_inputs() # Triggers validation for no inputs

    def __repr__(self):
        return '<Report %s %s>' % self.unique_id

    @property
    def unique_id(self):
        """
        A unique string for this report with the given user inputs.
        
        This string uniquely identifies the given report once the set of user
        inputs has been applied. This is used as a cache key prefix.
        """
        # If it has been set manually, use that
        if getattr(self, '_unique_id_override', None):
            return self._unique_id_override

        # Otherwise, determine it automatically from the inputs
        user_input_string = ''
        for user_input_name in sorted(self.dirty_inputs):
            user_input_string += '%s:%s,' % (
                user_input_name,
                str(self.dirty_inputs[user_input_name])
            )
        user_input_hash = hashlib.sha1(user_input_string).hexdigest()[::2]
        return self.code_name, user_input_hash

    @unique_id.setter
    def unique_id(self, value):
        self._unique_id_override = value

    @classmethod
    def render_widgets(cls):
        """
        Returns a list of this report's widgets, rendered to HTML.
        """
        return [widget.render() for name, widget in cls.widgets]

    @classmethod
    def get_widgets(cls):
        """
        Returns a list of this report's widgets, NOT rendered.
        You'll have to call .render() in the template.
        """
        return [widget for name, widget in cls.widgets]

    def clean_user_inputs(self, **kwargs):
        """
        Applies the user inputs and returns error messages, if any.
        
        These should be passed in as keyword arguments the way they are
        returned in the GET parameters produced by the widgets' HTML. This
        method then cleans the widgets and records any errors.
        
        If there were errors, they are returned as a list of strings. If not,
        returns None and stores the user inputs on the report. Note that this
        effectively changes the unique_id property of the report.
        """
        # Don't want to update self.user_inputs until we've validated them all
        dirty_inputs = self.dirty_inputs.copy()
        clean_inputs = {}
        errors = []
        for name, fil in self.filters:
            if fil.widget:
                name = fil.widget.form_name
                dirty_input = kwargs.get(name, dirty_inputs.get(name, None))
                if not dirty_input:
                    name = fil.widget._name
                    dirty_input = kwargs.get(name, dirty_inputs.get(name, None))

                try:
                    clean_input = fil.widget.clean(dirty_input)
                    clean_inputs[fil.widget._name] = clean_input
                except widgets.ValidationError as e:
                    errors.append(e)
                dirty_inputs[name] = dirty_input

        # Only update the report's user_inputs if they are error-free
        self.user_input_errors = errors
        if errors:
            self.clean_inputs = {}
        else:
            self.dirty_inputs = dirty_inputs
            self.clean_inputs = clean_inputs
        return errors

    def _init_footer(self):
        # Resets all the footer tracking info
        self._footer_finalized = False
        self._footer_increment_complete = False
        self._footer = dict([(name, None) for name in self.columns_dict.keys()])
        self._row_count = 0

    def _get_key_rows(self):
        # Returns an iterator of empty rows with each key.
        # For each key in the report, this will output a "full" row with that
        # key and all other values null.
        keys = [key.get_row_keys(self.clean_inputs) for _, key in self.keys]
        key_names = [name for name, _ in self.keys]
        return itertools.imap(
            lambda key: (key, dict(zip(key_names, key))),
            itertools.product(*keys)
        )

    def _get_rows(self):
        # Compile all the sources' get_rows
        source_rows = []
        # Tee key rows to save memory while all sources iterate in tandem
        # over the key rows iterator (one for each source, plus one to merge)
        teed_key_rows = itertools.tee(
            self._get_key_rows(), len(self._sources) + 1)
        for source, key_rows in zip(self._sources, teed_key_rows):
            source.pre_process(self.clean_inputs)
            source_rows.append(source.get_rows(key_rows, self.clean_inputs))

        # Empty rows for each key ensures every key gets a row
        # (Use the last teed key row for the merge)
        empty_row = dict(map(lambda a: (a[0], None), self.columns))
        source_rows.append(teed_key_rows[-1])

        # Merge the source rows into finalized rows
        current_row = None
        current_key = None
        for key, source_row in heapq.merge(*source_rows):
            if current_key and current_key == key:
                # Continue building the current row
                current_row.update(source_row)
            else:
                if current_key is not None:
                    # Done with the current row, so process and emit it
                    for source in self._sources:
                        current_row = source.post_process(
                            current_row, self.clean_inputs)
                    self._increment_footer(current_row)
                    yield current_row
                # Start building the next row
                current_key = key
                current_row = empty_row.copy()
                current_row.update(source_row)

        # Process and emit the last row, assuming we have any rows
        if current_row is not None:
            for source in self._sources:
                current_row = source.post_process(
                    current_row, self.clean_inputs)
            self._increment_footer(current_row)
            yield current_row

        # Mark that the footer has been fully incremented
        self._footer_increment_complete = True

    def _increment_footer(self, row):
        # Increments the column footers by the given row.
        self._row_count += 1

        # Run each column's footer increment function
        for key in self._footer.keys():
            self._footer[key] = self.columns_dict[key] \
                .increment_footer(self._footer[key], row[key])

    def _get_footer(self):
        # Retrieve the finalized footer for this report.
        if not self._footer_increment_complete:
            raise ValueError('You must exhaust report._get_rows before you '
                'can access the footer.')

        # Finalize the footer if it hasn't yet been
        if not self._footer_finalized:
            for key in self._footer.keys():
                self._footer[key] = self.columns_dict[key] \
                    .finalize_footer(self._footer[key], self._footer)
            self._footer_finalized = True

        # Return the finalized footer
        return self._footer

    def run_report(self):
        """
        Builds the instance in cache.
        
        This activates all the sources' pre_process, get_rows, and
        post_process methods and compiles the results. It also processes the
        footer. It passes this all into the caching function for storage.
        
        This can be time-consuming, and is best handled as a task outside of
        the request-response cycle.
        """
        # First reset footer totals, in case the same report is run twice
        self._init_footer()
        self.cache.create_instance(self.unique_id[0], self.unique_id[1],
            self._get_rows(), self._get_footer, self.cache_time)

    def kill_cache(self, full=False):
        """
        Removes the entire stored cache for this report instance.

        If the optional 'full' arguments is True then all cached data for this
        report will be cleared, otherwise just the cache data for the current
        user inputs will be cleared.
        """
        if full:
            self.cache.kill_report_cache(self.unique_id[0])
            return
        self.cache.kill_instance_cache(*self.unique_id)

    def is_report_started(self):
        """
        Returns True if the report has begun being stored in cache.
        """
        return self.cache.is_instance_started(*self.unique_id)

    def is_report_finished(self):
        """
        Returns True if the report is finished being run and cached.
        """
        return self.cache.is_instance_finished(*self.unique_id)

    def report_row_count(self):
        """
        Returns the total number of rows in this report.
        """
        return self.cache.instance_row_count(*self.unique_id)

    def report_timestamp(self):
        """Returns the timestamp when the report was originally run."""
        return self.cache.instance_timestamp(*self.unique_id)

    def report_header(self, format=None):
        """
        Returns the header data for this report.
        
        Takes an optional keyword argument for the output format.
        
        The first column returned by every blingalytics report is a hidden
        column containing the internal id of the row.
        
        The header info is returned as a list of dicts with the formatters'
        header info. By default, these header dicts contain the following
        information: 
        
        * label: The label to be displayed on the column.
        * alignment: Either 'left' or 'right' for the column alignment.
        * hidden: Either True or False to hide or show the column.
        * sortable: Either True or False for whether to allow sorting on this
          column.
        """
        header_row = []

        # First column is always the row id
        header_row.append({
            'key': '_bling_id',
            'label': 'Bling ID',
            'hidden': True,
            'sortable': False,
        })

        # Append the header info for the report columns
        for name, column in self.columns:
            header_info = column.format.header_info
            header_info.update(key=name)
            header_row.append(header_info)
        return header_row

    def report_rows(self, selected_rows=None, sort=None, limit=None, offset=0, format='html'):
        """
        Queries the cached table and returns the rows.
        
        The following options are available to sort and format the rows:
        
        * selected_rows: If you want to limit your results to only a subset of
          the report's rows, you can provide them as a list of row ids.
          Optional, defaults to None.
        * sort: This is a tuple to specify the sorting on the table, in the
          same format as the default_sort attribute on reports. That is, the
          first element should be the label of the column and the second
          should be either 'asc' or 'desc'. Optional, defaults to the sorting
          specified in the report's default_sort attribute.
        * limit: The number of rows to return. Defaults to None, which does
          not limit the results.
        * offset: The number of rows offset at which to start returning rows.
          Defaults to 0.
        * format: The type of formatting to use when processing the output, as
          defined in the formats instances. Defaults to 'html'.
        
        The rows are returned as a list of lists of values.
        
        The first value in each row returned by every blingalytics report is a
        hidden column containing the internal id of the row.
        """
        # Query for the raw row data
        sort = sort or self.default_sort
        alpha = getattr(dict(self.columns)[sort[0]], 'sort_alpha', False)
        raw_rows = self.cache.instance_rows(self.unique_id[0],
            self.unique_id[1], selected=selected_rows, sort=sort, limit=limit,
            offset=offset)

        # Format the row data
        formatted_rows = []
        for raw_row in raw_rows:
            formatted_row = []

            # First column is always the row id
            formatted_row.append(raw_row['_bling_id'])

            # Format and append the report columns
            for name, column in self.columns:
                format_fn = getattr(column.format, 'format_%s' % format,
                    column.format.format)
                formatted_cell = format_fn(raw_row[name])
                formatted_row.append(formatted_cell)
            formatted_rows.append(formatted_row)

        return formatted_rows

    def report_footer(self, format='html'):
        """
        Returns the footer row for the table.
        
        Accepts an optional keyword argument for the output format, which
        defaults to 'html'.
        
        Returns the formatted footer row as a list, including hidden column
        data. The first column, which is for the internal row ids, is left
        empty for the footer.
        """
        # Query for the footer data
        footer_row = self.cache.instance_footer(*self.unique_id)

        # Format the footer data (first is always the row id)
        formatted_footer = [None]
        for key, column in self.columns:
            if column.footer:
                format_fn = getattr(column.format, 'format_%s' % format,
                    column.format.format)
                formatted_cell = format_fn(footer_row[key])
                formatted_footer.append(formatted_cell)
            else:
                formatted_footer.append(None)

        return formatted_footer
