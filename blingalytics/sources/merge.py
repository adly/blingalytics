"""
The merge source provides a mechanism for merging and filtering the data
resulting from two or more other "sub-reports". This can be useful if you need
to combine results from two different databases or with two different key
ranges into one report.

When building a merged report, you must specify the reports to merge in a
report attribute:

* ``merged_reports``: Specifies the sub-reports you want to merge. Provide the
  reports as a dict with the keys naming the sub-reports and the values being
  the sub-report classes themselves. For example::

      merged_reports = {
          'revenue': ProductRevenueReport,
          'awesome': UserAwesomenessReport,
      }

All merge columns take the same positional arguments, which are used to
specify which columns from sub-reports should be combined into the merge
column. You can specify the merge columns as follows:

* If no argument is provided, the merge report will merge any columns found
  with the same name from all sub-reports.
* If you provide a single string as an argument, the merge report will merge
  any columns from sub-reports that have that name.
* To merge columns of various names from various sub-reports, you can specify
  as many as you like as dotted strings. Each string should be in the form of
  ``'report_name.column_name'``.

As a merged report is processed, it will actually run the full end-to-end
``run-report`` process for each of its sub-reports. It will then aggregate
the results together based on the columns and filters in the merge report.
"""

import heapq

from blingalytics import base, sources


class MergeSource(sources.Source):
    def __init__(self, report):
        super(MergeSource, self).__init__(report)
        if len(self._keys) != 1:
            raise ValueError('Merge reports cannot have more than one key or '
                'we are unable to ensure proper sorting of the subreports.')
        self._report = report
        self.set_merged_reports(report.merged_reports)

    def set_merged_reports(self, merged_reports):
        # Receive the reports to merge and instantiate them
        self._reports = {}
        for name, merged_report in merged_reports.iteritems():
            # TODO: Handle instantiation from dicts
            # if isinstance(merged_report, dict):
            #     ReportMeta.
            if not isinstance(merged_report, base.Report):
                merged_report = merged_report(self._report.cache, merge=True)
            self._reports[name] = merged_report

    def _report_rows_mapper(self, report):
        # For a report, returns an iterator over its report_rows method that
        # maps the rows to the ((key), 'report_name', {row}) format required
        # for sorting by the heapq.merge function. Note that the key will be
        # pulled based on the merge report's key, not the subreports' key,
        # and we ensure the output is sorted by that key so merge works.
        key = self._keys[0][0]
        index = None
        column_names = []
        for i, header in enumerate(report[1].report_header()):
            column_names.append(header['key'])
            if header['key'] == key:
                index = i
        for row in report[1].report_rows(sort=(key, 'asc'), format='raw'):
            row_key = (row[index],)
            report_name = report[0]
            row_dict = dict(zip(column_names, row))
            yield (row_key, report_name, row_dict)

    def _passes_post_filters(self, row, clean_inputs):
        # Returns true if the row passes all the report's PostFilters.
        return all([
            fil.include_row(row, clean_inputs)
            for name, fil in self._filters if isinstance(fil, PostFilter)
        ])

    def get_rows(self, key_rows, clean_inputs):
        # Apply the delegated report filters
        for name, report in self._reports.items():
            # Override the sub-report's filters and widgets
            sub_dirty_inputs = {}
            for key, value in self._report.dirty_inputs.iteritems():
                sub_key = key.replace(self._report.code_name, report.code_name)
                sub_dirty_inputs[sub_key] = value
            report.clean_user_inputs(**sub_dirty_inputs)
            # Override the report's default unique_id
            report_id, instance_id = self._report.unique_id
            report.unique_id = (report_id, '%s::%s' % (instance_id, name))

        empty_row = dict(map(lambda a: (a[0], None), self._columns))
        current_key = None
        current_row = None

        # Collect the reports that are not excluded by filtering
        excluded_reports = []
        for name, fil in self._filters:
            if isinstance(fil, ReportFilter):
                excluded_reports += fil.excluded_reports(clean_inputs)
        reports = [
            (name, report) for name, report in self._reports.items()
            if name not in excluded_reports
        ]

        # Run the reports synchronously so we can query them
        for name, report in reports:
            if not report.is_report_finished():
                report.run_report()

        # Prep the reports' rows for iteration with heapq
        # Must be in the form ((key), 'report_name', {row})
        report_rows = map(self._report_rows_mapper, reports)

        for key, report, row in heapq.merge(*report_rows):
            if current_key and current_key == key:
                # Continue building the current row
                for name, column in self._columns:
                    current_row[name] = column._merge_report_column(
                        report, name, current_row, row)
            else:
                if current_key is not None:
                    # Done with the current row, so emit it
                    if self._passes_post_filters(current_row, clean_inputs):
                        yield (current_key, current_row)
                # Start building the next row
                current_key = key
                current_row = empty_row.copy()
                for name, column in self._columns:
                    current_row[name] = column._merge_report_column(
                        report, name, current_row, row)

        # Emit the final row, if any
        if current_key:
            if self._passes_post_filters(current_row, clean_inputs):
                yield (current_key, current_row)

class DelegatedFilter(sources.Filter):
    """
    Allows you to display one widget from the merge report, then supply the
    user input to each of the sub-reports to process as they normally would
    using the filters defined on each sub-report.

    This filter simply takes the standard widget keyword argument. You also
    need to ensure that the name of this filter matches the name of any
    sub-report filters you want to pick up the user input.
    """
    def __init__(self, *args, **kwargs):
        super(DelegatedFilter, self).__init__(**kwargs)

class PostFilter(sources.Filter):
    """
    This filter can be used to exclude entire rows from the output, based on
    the data in the row from the sub-reports.

    This takes one positional argument, which should be a filtering function.
    The function takes the merged row as a dict, and optionally the user input
    if a widget is specified. If the function returns a truthy value, the row
    will be included; otherwise, it will be skipped. For example::

        merge.PostFilter(
            lambda row, user_input: row['revenue'] >= user_input,
            widget=widgets.Select(choices=MIN_REVENUE_CHOICES))

    """
    def __init__(self, filter_func, **kwargs):
        self.filter_func = filter_func
        super(PostFilter, self).__init__(**kwargs)

    def include_row(self, row, clean_inputs):
        if self.widget:
            user_input = clean_inputs[self.widget._name]
            return self.filter_func(row, user_input)
        return self.filter_func(row)

class ReportFilter(sources.Filter):
    """
    This filter allows you to selectively include or exclude specific
    sub-reports from being processed at all. It takes one positional argument:
    the name of the sub-report from the ``merged_reports`` report attribute
    from the merge report.

    This filter requires a widget (it would be pretty silly to use this one
    without a widget). It will include the specified sub-report in the merged
    output only if the user input is truthy. Generally, a
    :class:`Checkbox <blingalytics.widgets.Checkbox>` widget is appropriate
    for this.
    """
    def __init__(self, report_name, **kwargs):
        if 'widget' not in kwargs:
            raise ValueError('ReportFilter requires a widget.')
        self.report_name = report_name
        super(ReportFilter, self).__init__(**kwargs)

    def excluded_reports(self, clean_inputs):
        if clean_inputs.get(self.widget._name):
            return []
        return [self.report_name]

class MergeColumn(sources.Column):
    """
    Base class for merge report columns.
    
    All merge columns take the same positional arguments to specify which
    columns from sub-reports should be combined into this one merged column,
    as follows.
    
    If no column name is provided, it will be assumed to be the same as the
    name of this merged report column on all sub-reports.
    
    If you want to specify the name of the column in sub-reports, and it is
    the same name in each, you can simply provide the column name as a string
    as the first argument to the merge column.
    
    If you want to combine columns of different names in each of the
    sub-reports, you can provide multiple strings as keyword arguments, with
    each string specifying the sub-report name and column name, in the form of
    'report_name.column_name'.
    
    Subclasses must all implement the merge method, which implements the
    specific sub-report merging functionality for the column.
    """
    source = MergeSource

    def __init__(self, *args, **kwargs):
        # Parse the columns to be merged from sub-reports
        self._merge_all = None
        self._merge_columns = []
        if len(args) == 1 and '.' not in args[0]:
            self._merge_all = args[0]
        else:
            for arg in args:
                self._merge_columns.append((arg.rsplit('.', 1)))
        super(MergeColumn, self).__init__(**kwargs)

    def _merge_report_column(self, report_name, column_name, current, new):
        # Determines whether this report should be merged, and if so, performs
        # the merge method over the values.
        if not self._merge_columns and not self._merge_all:
            # Use the merge report's column name
            return self.merge(current.get(column_name), new.get(column_name))
        elif self._merge_all and self._merge_all in new:
            # Use the provided column name for the incoming row
            return self.merge(current.get(column_name), new.get(self._merge_all))
        elif self._merge_columns:
            # Use the specified merge columns if they apply, otherwise skip (return current)
            for merge_report_name, merge_column_name in self._merge_columns:
                if merge_report_name == report_name and merge_column_name in new:
                    return self.merge(current.get(column_name), new.get(merge_column_name))
            return current.get(column_name)
        else:
            # Column not specified for a merge, so skip (return current)
            return current.get(column_name)

    def merge(self, current, new):
        """
        Merges all values returned by sub-reports for this column.
        
        This method should be implemented by concrete merge column types to
        perform their column-specific merging strategy.
        
        The method is called once for each sub-report that returns a value for
        this column. The method receives the current merged value, which is
        either None if this is the first time it is called for a report, or is
        the current merged value it last returned. It also receives the new
        value returned by the current report. It should return the resulting
        merged value.
        """
        raise NotImplementedError

class First(MergeColumn):
    """
    Merges sub-report columns by keeping the first value returned by any
    sub-report. Takes the standard merge column arguments, as described above.
    """
    def merge(self, current, new):
        if current is None:
            return new
        return current

class Sum(MergeColumn):
    """
    Merges sub-report columns by summing the values returned by each
    sub-report. Takes the standard merge column arguments, as described above.
    """
    def merge(self, current, new):
        if current is None and new is None:
            return None
        if current is None:
            return new
        if new is None:
            return current
        return current + new

# IS THIS NECESSARY? HELPFUL? CAN'T BE DONE WITH MERGE METHOD AS IT IS NOW
# class Count(MergeColumn):
#     """Computes the count of these columns for the row key."""
#     def merge(self, current, new):
#         pass

class BoolAnd(MergeColumn):
    """
    Merges sub-report columns by performing a boolean-and operation over the
    values returned by the sub-reports. This returns ``True`` if *all* the
    values from sub-reports are truthy; otherwise, it returns ``False``. A
    ``None`` value is considered ``True`` here so that they essentially are
    ignored in determining the result. Takes the standard merge column
    arguments, as described above.
    """
    def merge(self, current, new):
        current = bool(current) if current is not None else True
        new = bool(new) if new is not None else True
        return current and new

    def increment_footer(self, total, cell):
        # No footer for boolean columns
        return None

    def finalize_footer(self, total, footer):
        # No footer for boolean columns
        return None

class BoolOr(MergeColumn):
    """
    Merges sub-report columns by performing a boolean-or operation over the
    values returned by the sub-reports. This returns ``True`` if *any* of the
    values from sub-reports are truthy; otherwise, it returns ``False``. A
    ``None`` value is considered ``False`` here so that they essentially are
    ignored in determining the result. Takes the standard merge column
    arguments, as described above.
    """
    def merge(self, current, new):
        current = bool(current) if current is not None else False
        new = bool(new) if new is not None else False
        return current or new

    def increment_footer(self, total, cell):
        # No footer for boolean columns
        return None

    def finalize_footer(self, total, footer):
        # No footer for boolean columns
        return None
