"""
Reports are used to define a view into your data. At a basic level, reports
are used to process a data set, format the data, and return tabular results.
But reports can define many options, including:

* The name of the report
* The category of the report
* How long to cache the report
* What source data to pull
* How to manipulate the source data
* How the output should be formatted
* What options are given to the user to filter the data
* The default sorting

The next section describes in detail how to write and interact with your
own reports. You will also want to check out :doc:`/sources` for details on
how to pull data; :doc:`/formats` for more on formatting report output; and
:doc:`/widgets` for details on accepting user filtering options.
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
        filters = cls._get_defined(bases, dct, 'filters', [])
        for fil_name, fil in filters:
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

        dct['keys'] = cls._get_defined(bases, dct, 'keys', [])
        dct['columns'] = cls._get_defined(bases, dct, 'columns', [])

        dct['category'] = cls._get_defined(bases, dct, 'category')
        dct['database_entity'] = cls._get_defined(bases, dct, 'database_entity')
        dct['default_sort'] = cls._get_defined(bases, dct, 'default_sort')

        # Keep a list of all the reports that exist
        report_cls = type.__new__(cls, name, bases, dct)
        cls.report_catalog.append(report_cls)

        return report_cls

    @staticmethod
    def _get_defined(bases, dct, name, default=None):
        if name in dct:
            return dct.get(name)
        for base in bases:
            if hasattr(base, name):
                return getattr(base, name)
        return default

class Report(object):
    """
    To write a report, you subclass this base Report class and define your own
    attributes on it. The standard list of available attributes include:
    
    ``category`` *(optional)*
        A string representing the category this report belongs to. The
        category is used by the get_reports_by_category function to group like
        reports together.

    ``display_name`` *(optional)*
        The title of the report as a string. If not specified, this will be
        generated automatically based on the class name.

    ``code_name`` *(optional)*
        The code name of the report as a string. The code name is used to
        identify this report, for example as part of a key name when caching
        the report's data. If not specified, this will be generated
        automatically based on the class name.

    ``cache_time`` *(optional)*
        The number of seconds this report should remain valid in the cache.
        If not specified, defaults to ``1800`` (30 minutes).

    ``keys``
        If your report has just one key, this should be a two-tuple: the name
        of the key column as a string; and the desired key range class or
        instance. If you want a report with compound keys, you can specify
        them as a list of these two-tuples. This is described in detail in
        :doc:`/sources/key_range`.

    ``columns``
        This should be a list of two-tuples describing your report's columns.
        The first item of each two-tuple is the name of the column as a
        string. The second item is a column class or instance. The column
        definitions do all the heavy lifting of pulling data from sources,
        manipulating it, and formatting the output. For more details, see
        :doc:`/sources`.

    ``filters`` *(optional)*
        A list of two-tuples describing the filters and widgets to present to
        your users. The first item of each two-tuple is the name of the filter
        as a string. The second item is a filter instance. The types of
        filters you can use are specific to the sources you're using; see the
        relevant source's documentation in :doc:`/sources`. Filters will also
        generally specify a widget for collecting the user input; for more,
        see :doc:`/widgets`.

    ``default_sort`` *(optional)*
        A two-tuple representing the column and direction that the report
        should be sorted by default. The first item is the name of the column
        to sort on, as a string. The second is the sort direction, as either
        ``'asc'`` or ``'desc'``. If not specified, this defaults to sorting
        by the first column, descending.

    Various sources used by the report may allow or require other attributes
    to be specified. This will be specified in the documentation for that
    source.

    Here is a relatively simple example of a report definition::

        from blingalytics import base, formats, widgets
        from blingalytics.sources import database, derived, key_range

        class RevenueReport(base.Report):
            display_name = 'Company Revenue'
            code_name = 'company_revenue_report'
            category = 'business'
            cache_time = 60 * 60 * 3 # three hours

            database_entity = 'project.models.reporting.RevenueModel'
            keys = ('product_id', key_range.SourceKeyRange)
            columns = [
                ('product_id', database.GroupBy('product_id',
                    format=formats.Integer(label='ID', grouping=False), footer=False)),
                ('product_name', database.Lookup('project.models.products.Product',
                    'name', 'product_id', format=formats.String)),
                ('revenue', database.Sum('purchase_revenue', format=formats.Bling)),
                ('_cost_of_revenue', database.First('product_cost')),
                ('gross_margin', derived.Value(
                    lambda row: (row['revenue'] - row['_cost_of_revenue']) * \\
                    Decimal('100.00') / row['revenue'], format=formats.Bling)),
            ]
            filters = [
                ('delivered', database.QueryFilter(
                    lambda entity: entity.is_delivered == True)),
                ('online_only', database.QueryFilter(
                    lambda entity, user_input: entity.is_online_purchase == user_input,
                    widget=widgets.Checkbox(label='Online Purchase'))),
            ]
            default_sort = ('gross_margin', 'desc')

    Once you have defined your report subclass, you instantiate it by passing
    in a cache instance. This tells the report how and where to cache its
    processed data. For more, see :doc:`/caches`. Once you have a report
    instance, you use the following methods to run it, manipulate it, and
    retrieve its data.
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
        fallback_sort = (self.columns[0][0], 'desc') if self.columns else None
        self.default_sort = getattr(self, 'default_sort', fallback_sort)
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
        widget_unique_ids = []
        for name, widget in self.widgets:
            widget_unique_ids.append(widget.get_unique_id(self.dirty_inputs))
        user_input_string = ":".join(sorted(widget_unique_ids))

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

    def get_widget_choices(self):
        widget_choices = {}
        for key, fil in self.filters:
            if fil.widget and hasattr(fil.widget, 'choices'):
                widget_choices[key] = fil.widget.get_choices()
        return widget_choices

    def override_widget_choices(self, **kwargs):
        for key, value in kwargs.items():
            for widget in self.widgets:
                if widget[0] == key:
                    widget[1].choices = value

    @classmethod
    def get_widgets(cls):
        """
        Returns a list of this report's widget instances. Calling the widgets'
        ``render()`` method is left to you.
        """
        return [widget for name, widget in cls.widgets]

    def clean_user_inputs(self, **kwargs):
        """
        Set user inputs on the report, returning a list of any validation
        errors that occurred.

        The user input should be passed in as keyword arguments the same as
        they are returned in a GET or POST from the widgets' HTML. The widgets
        will be cleaned, converting to the appropriate Python objects.

        If there were errors, they are returned as a list of strings. If not,
        returns ``None`` and stores the user inputs on the report. Note that
        this effectively changes the ``unique_id`` property of the report.

        If your report has user input widgets, this should be called before
        run_report; if the report has no widgets, you don't need to call this
        at all.
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
        Processes the report data and stores it in cache.

        Depending on the size of the data and the processing going on, this
        call can be time-consuming. If you are deploying this as part of a
        web application, it's recommended that you perform this step outside
        of the request-response cycle.
        """
        # First reset footer totals, in case the same report is run twice
        self._init_footer()
        self.cache.create_instance(self.unique_id[0], self.unique_id[1],
            self._get_rows(), self._get_footer, self.cache_time)
        self.report_finalize()

    def kill_cache(self, full=False):
        """
        By default, this removes or invalidates (depending on the cache store
        being used) this cached report. If there are other cached versions of
        this report (with different user inputs) they are left unchanged.

        If you pass in ``full=True``, this will instead perform a full
        report-wide cache invalidation. This means any version of this report
        in cache, regardless of user inputs, will be wiped.
        """
        if full:
            self.cache.kill_report_cache(self.unique_id[0])
            return
        self.cache.kill_instance_cache(*self.unique_id)

    def is_report_started(self):
        """
        If :meth:`run_report` is currently running, this returns ``True``;
        otherwise, ``False``.
        """
        return self.cache.is_instance_started(*self.unique_id)

    def is_report_finished(self):
        """
        If :meth:`run_report` has completed and there is a current cached copy
        of this report, returns ``True``; otherwise, ``False``.
        """
        return self.cache.is_instance_finished(*self.unique_id)

    def report_row_count(self):
        """
        Returns the total number of rows in this report instance.
        """
        return self.cache.instance_row_count(*self.unique_id)

    def report_timestamp(self):
        """
        Returns the timestamp when the report instance was originally computed
        and cached, as a ``datetime`` object.
        """
        return self.cache.instance_timestamp(*self.unique_id)

    def report_header(self):
        """
        Returns the header data for this report, which describes the columns
        and how to display them.

        The header info is returned as a list of dicts, one for each column,
        in order. Certain column types may add other relevant info, but by
        default, the header dicts will contain the following:

        * ``label``: The human-readable label for the column.
        * ``alignment``: Either ``'left'`` or ``'right'`` for the preferred
          text alignment for the column.
        * ``hidden``: If ``True``, the column is meant for internal use only
          and should not be displayed to the user; if ``False``, it should be
          shown.
        * ``sortable``: If ``True``, this column can be sorted on.

        The first column returned by a Blingalytics report is always a hidden
        column specifying the row's cache ID. The first column header is for
        this internal ID.
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
        Returns the requested rows for the report from cache. There are a
        number of arguments you can provide to limit, order and format the
        rows, all of which are optional:

        * ``selected_rows``: If you want to limit your results to a subset of
          the report's rows, you can provide them as a list of internal cache
          row IDs (the ID of a row is always returned in the row's first
          column). Defaults to ``None``, which does not limit the results.
        * ``sort``: This is a two-tuple to specify the sorting on the table,
          in the same format as the ``default_sort`` attribute on reports.
          That is, the first element should be the label of the column and the
          second should be either ``'asc'`` or ``'desc'``. Defaults to the
          sorting specified in the report's ``default_sort`` attribute.
        * ``limit``: The number of rows to return. Defaults to ``None``, which
          does not limit the results.
        * ``offset``: The number of rows offset at which to start returning
          rows. Defaults to ``0``.
        * ``format``: The type of formatting to use when processing the
          output. The built-in options are ``'html'`` or ``'csv'``. Defaults
          to ``'html'``. This is discussed in more detail in :doc:`/formats`.
        
        The rows are returned as a list of lists of values. The first column
        returned by Blingalytics for any report is always a hidden column
        specifying the row's internal cache ID.
        """
        # Query for the raw row data
        sort = sort or self.default_sort
        alpha = getattr(dict(self.columns)[sort[0]], 'sort_alpha', False)
        raw_rows = self.cache.instance_rows(self.unique_id[0],
            self.unique_id[1], selected=selected_rows, sort=sort, limit=limit,
            offset=offset, alpha=alpha)

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

    def report_finalize(self):
        """
        Cleans up source columns.
        """
        [c.finalize() for c in self.columns_dict.values()]

    def report_footer(self, format='html'):
        """
        Returns the computed footer row for the report. There is one argument
        to control the formatting of the output:

        * ``format``: The type of formatting to use when processing the
          output. The built-in options are ``'html'`` or ``'csv'``. Defaults
          to ``'html'``. This is discussed in more detail in :doc:`/formats`.

        The footer row is returned as a list, including the data for any
        hidden columns. The first column returned by Blingalytics for any
        report is reserved for the row's internal cache ID, so the first item
        returned for the footer will always be ``None``.
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
