"""
Sources implement an interface between the report and the original data.

Based on the columns provided in the report definition, a report has one
instance of each type of source. Each of these sources describes how to grab
source data and manipulate it to produce the report.

For example, the database source provides an interface for doing sums and
counts over database columns; the derived source allows you to perform
calculations over columns from other sources; and the merge source allows
you to pull data from other reports and produce a sort of meta-report.

All source implementations should provide a subclass of Source; they should
subclass Filter for any filter types the source provides; and they should
subclass Column for any column types the source provides.
"""

from datetime import datetime, timedelta
from decimal import Decimal

from dateutil import relativedelta

from blingalytics import formats
from blingalytics.utils import epoch


ADD_TYPES = (int, long, Decimal, float)

class Source(object):
    """
    Defines the base interface for a report to access a data source.

    Subclasses should define any or all of the methods: pre_process, get_rows,
    and post_process. The report base class will call these methods at the
    appropriate times to retrieve and process the data from this source.
    
    The pre_process method is called at the start of the get_rows call to the
    report instance. You can use this method to do any setup or processing to
    prepare for the get_rows call.
    
    The get_rows method is called by the report instance on all its sources in
    tandem, and they are combined using Python's heapq implementation. See the
    get_rows docstring for a detailed explanation of how to emit the rows.
    
    The post_process gives each source an opportunity to act on the row values
    as returned by itself and all the other sources. This provides an
    opportunity for the source to do calculations or adjustments based on the
    values returned by another source.
    
    By default, the Source class stores the report's filters, key ranges, and
    columns, for reference within the source implementation:
    
    * _filters: The filters list defined on the report.
    * _keys: The normalized list of key ranges on the report.
    * _report_columns: The list of columns defined on the report.
    * _report_columns_dict: A dict version of the report columns that you can
      use when the column order doesn't matter and a dict is more convenient.
    * _columns: The list of columns belonging to this source that are defined
      on the report, in the order they appear.
    * _columns_dict: A dict version of the _columns attribute.
    """
    def __init__(self, report):
        self.set_filters(getattr(report, 'filters', []))
        self.set_keys(report.keys)
        self.set_columns(report.columns)

    def set_filters(self, filters):
        self._filters = filters

    def set_keys(self, keys):
        self._keys = normalize_key_ranges(keys)

    def set_columns(self, columns):
        """
        Stores the columns defined on the source's report.

        The original list of tuples will be stored as self._columns. For
        convenience, this will also be converted to a dict and stored as
        self._columns_dict. Additionally, registers all the columns from this
        source type.
        """
        self._report_columns = columns
        self._report_columns_dict = dict(self._report_columns)
        self._columns = []
        for name, column in self._report_columns:
            if isinstance(self, column.source):
                self._columns.append((name, column))
        self._columns_dict = dict(self._columns)

    def pre_process(self, clean_inputs):
        """
        Hook for doing prep work for the source.
        
        You can use this method to do any setup or processing to prepare for
        the get_rows call.
        """
        pass

    def get_rows(self, key_rows, clean_inputs):
        """
        Returns an iterator over the row data from this source.
        
        The formatting and order of the returned rows is hugely important. The
        report instance pulls the iterators for all its sources' get_rows in
        tandem, and combines them into rows using Python's heapq
        implementation. 
        
        Rows should be returned as a tuple of key and partial row. The key is
        a tuple of the values of the report's key columns, in the order they
        are defined in report.keys. (Every source in a report is required to
        return the same keys.) The partial row is a dict of column name to
        value pairs. The rows must be returned in sorted order (by the key
        tuple). For example:
        
        [
            ((12,), {'date': 12, 'total': 3}),
            ((14,), {'date': 14, 'total': 35}),
            ((15,), {'date': 15, 'total': 10}),
            ...
        ]
        
        The get_rows method is passed the key_rows for the report, which are
        rows (formatted as above) that just contain the key columns. This can
        be useful if the source needs to act upon these values, for example to
        perform database lookups on the values. However, the report is not
        required to use these values and need not even emit a value for each
        key row.
        
        It is also good practice to process the rows as incrementally as
        possible. Since we are often working with very large data sets, we
        don't want to pull the whole thing into memory all at once. Instead,
        it is often preferable for get_rows to be a generator that processes
        and yields the data in batches.
        """
        return []

    def post_process(self, row, clean_inputs):
        """
        Hook for doing any post-processing work. 
        
        This method is called once for each row in the report, once it has
        combined the results of the get_rows from each of the report's
        sources. It receives the row as a dict, and it should return the full
        row after it has made the desired updates.
        
        This method allows for any processing work over the results from this
        and other sources. It should not, however, rely on the post_process
        work from any sources, as post_process is called in an indeterminate
        order for all the report's sources.
        """
        return row

class Filter(object):
    """
    Defines the base for filtering source data.
    
    By default, filters take two optional arguments:
    
    * columns: Can be a string, a list of strings, or None. If one or more
      strings are provided, the filter should be applied only to those columns
      in the report. If None is provided (the default) the filter should be
      applied report-wide.
    * widget: A widget class or instance that defines the widget type the user
      should be shown to input a filter argument. This is optional, and
      defaults to None.
    
    This base class, however, merely accepts these as standard filtering
    options. It is entirely up to the subclassed filter and source
    implementations to define the functionality of the columns and widgets.
    """
    def __init__(self, columns=None, widget=None):
        # Using frozensets for column lists so they can be used as dict keys
        if isinstance(columns, basestring):
            self.columns = frozenset([columns])
        elif columns:
            self.columns = frozenset(columns)
        else:
            self.columns = None
        self.widget = widget

class Column(object):
    """
    Defines the base for defining a column of this source type.
    
    Concrete subclasses are expected to set 'source' equal to their source
    class as an attribute on the column class. For example, for a database
    column would define the following line:
    
    source = DatabaseSource
    
    By default, columns handle two optional arguments:
    
    * format: A format class or instance that should be used to determine the
      formatting and display options for this column. If none is provided (the
      default), the column is assumed to be hidden.
    * footer: Whether or not to calculate and display footer totals for this
      column. Column subclasses may also implement special-case strings for
      this option to provide any other sort of optional footer functionality.
      Defaults to True.
    
    This base class, however, only implements basic format and footer
    handling. It is entirely up to the subclassed column and source
    implementations to define this column's specific functionality.
    
    By default, a column's footer functionality is simply a sum for types that
    can be added, and blank for all other values. For int, long, Decimal, and
    float values, all the values returned from the source are totaled and
    returned for the footer.
    
    To provide a specialized footer behavior for your column, you can override
    the increment_footer and finalize_footer methods, documented below.
    """
    def __init__(self, format=None, footer=True):
        # Normalize and provide defaults for options
        if format:
            if not isinstance(format, formats.Format):
                format = format()
        else:
            format = formats.Hidden()
        self.format = format
        self.footer = footer

    @property
    def sort_alpha(self):
        return getattr(self.format, 'sort_alpha', False)

    def increment_footer(self, total, cell):
        """
        Increments this column's footer total.
        
        Receives the current total, which is either None the first time this
        is called for a report, or whatever value the method returned last
        time. Also receives the cell value for this column for the row
        currently being processed.
        
        The method should return the new running total, incremented (using
        whatever method is appropriate) by the cell value.
        """
        if self.footer:
            if type(total) in ADD_TYPES:
                if type(cell) in ADD_TYPES:
                    return total + cell
                else:
                    return total
            else:
                if type(cell) in ADD_TYPES:
                    return cell
        return None

    def finalize_footer(self, total, footer):
        """
        Finalizes and returns this column's footer total.
        
        Receives the column's running total after increment_footer has been
        called for every row in the report. Also receives the entire footer's
        running totals as a dict.
        
        This allows your footer to be calculated by using data from other
        columns' footers. Your method should return the finalized footer total
        for this column.
        """
        return total

def normalize_key_ranges(init_key_ranges):
    """
    Utility function to normalize the keys given on a report definition.
    
    The key range argument should be a list of two-tuples specifying each key.
    The first element of the two-tuple is a string for the key's column; the
    second element should be a KeyRange instance.
    
    This attribute on a report is very permissive, though. The common case is
    to have just one key range for a report, so if you pass just the one
    two-tuple, it will automatically be wrapped in a list. You are also not
    required to instantiate the KeyRange object, which will be automatically
    instantiated here.
    """
    if isinstance(init_key_ranges[0], basestring):
        init_key_ranges = [init_key_ranges]
    key_ranges = []
    for name, key_range in init_key_ranges:
        if not isinstance(key_range, KeyRange):
            key_range = key_range()
        key_ranges.append((name, key_range))
    return key_ranges

class KeyRange(object):
    """
    Base class for key ranges.
    
    Key ranges define the keyspace for a report. In other words, this defines
    what rows you want outputted for a report, and the data will be pulled and
    filled in for those rows.

    For example, if you want to report on your revenue numbers by day, the key
    range will essentially be the list of days you want your renenue numbers
    for.

    You can also have a "compound key range" if you define a list of key
    ranges for your report. So for example, you could have a date key range
    and a publisher key range. This would mean you get one row per date per
    publisher.

    All key ranges should derive from this base KeyRange class. All subclasses
    should define the get_row_keys method to return an iterable of keys that
    must be in the report.
    """
    def get_row_keys(self, clean_inputs):
        """
        Returns an iterable of keys guaranteed for the report.
        
        This method is given the list of widgets from the report, from which
        it can pull user inputs for use in determining the key range. For
        example, a date key range could use start and end date widgets and
        return a key for each day in that range.
        
        Note that key ranges are not restrictive. That is, the keys returned
        by the key range guarantee that such a row will appear in the report
        output; however, a report may return more key values than defined in
        this key range. If a report's sources return a value for a given key
        column, that value will be included even if it is not returned as part
        of the key range.
        """
        raise NotImplementedError

class MonthRange(KeyRange):
    """
    Ensures a key for every month between the start and end dates.
    
    This key range takes two positional arguments, start and end, which are
    used to determine the range of months. These arguments can be datetimes,
    which will be used as-is; or they can be strings, which will be considered
    as references to named widgets, and the user input from the widget will be
    used for the date.
    
    The values of the keys returned by this key range are in the form of an
    integer representing the number of full days since the UNIX epoch
    (Jan. 1, 1970).
    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def _resolve_date(self, date, clean_inputs):
        if isinstance(date, basestring):
            # Resolve from the widget with the same name
            return clean_inputs[date]
        if isinstance(date, datetime):
            return date.date()
        return date

    def get_row_keys(self, clean_inputs):
        date = self._resolve_date(self.start, clean_inputs)
        date = date.replace(day=1)
        end = self._resolve_date(self.end, clean_inputs)
        while date <= end:
            yield date
            date += relativedelta.relativedelta(months=1)

class EpochKeyRange(KeyRange):
    """
    Ensures a key for every day between the start and end dates.
    
    This key range takes two positional arguments, start and end, which are
    used to determine the range of days. These arguments can be datetimes,
    which will be used as-is; or they can be strings, which will be considered
    as references to named widgets, and the user input from the widget will be
    used for the date.
    
    The values of the keys returned by this key range are in the form of an
    integer representing the number of full days since the UNIX epoch
    (Jan. 1, 1970).
    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def _resolve_date(self, date, clean_inputs):
        if isinstance(date, basestring):
            # Resolve from the widget with the same name
            try:
                return clean_inputs[date]
            except KeyError:
                raise ValueError('Start or end date name not found in widgets.')
        if isinstance(date, datetime):
            return date.date()
        return date

    def get_row_keys(self, clean_inputs):
        date = self._resolve_date(self.start, clean_inputs)
        end = self._resolve_date(self.end, clean_inputs)
        if date > end:
            raise ValueError('Start date must be earlier than end date.')
        while date <= end:
            yield epoch.datetime_to_hours(date) / 24
            date += timedelta(days=1)

class SourceKeyRange(KeyRange):
    """
    Doesn't ensure any keys.
    
    This key range essentially just allows for each key value returned from
    the report's sources to be a row. It does not produce any extra rows not
    returned by the sources.
    """
    def get_row_keys(self, clean_inputs):
        return []
