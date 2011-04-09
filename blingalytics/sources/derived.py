"""
Implements a data source pulling from other columns in the same report.

This source provides a way to do calculations over the other values in a
report returned by other sources. For example, if you have a report with
columns for gross revenue and net revenue, both pulled from the database
source, you could use a derived column to provide a gross margin column by
performing the operation (net_revenue / gross_revenue * 100).

Columns:

* derived.Value: Performs an arbitrary operation over other columns in a
  report row and returns the result.

No filters.
"""

import decimal

from blingalytics import sources


DIVISION_BY_ZERO = (decimal.InvalidOperation, ZeroDivisionError)

class DerivedSource(sources.Source):
    def post_process(self, row, clean_inputs):
        # Compute derived values for all columns on this row
        for name, column in self._columns:
            row[name] = column.get_derived_value(row)
        return row

class DerivedColumn(sources.Column):
    source = DerivedSource

class Value(DerivedColumn):
    """
    A column that derives its value from other columns in the row.
    
    The column takes one positional argument, which should be the function
    used to derive the column's value. This function will be passed one
    argument, the row as a dict as compiled from the get_rows method of all
    the report's sources. (This method is evaluated as part of the 
    post_process step in source evaluation.) The function should return the
    derived value.
    
    The footer for derived value columns by default performs the same
    operation over the appropriate footer columns to produce a footer result.
    This is generally the footer you want for a derived column, as opposed to
    simply summing or averaging the values in the column.
    """
    def __init__(self, derive_func, **kwargs):
        self.derive_func = derive_func
        super(Value, self).__init__(**kwargs)

    def get_derived_value(self, row):
        try:
            return self.derive_func(row)
        except TypeError:
            # Got None for a value, so return None
            return None
        except DIVISION_BY_ZERO:
            return decimal.Decimal('0.00')

    def increment_footer(self, total, cell):
        return None

    def finalize_footer(self, total, footer):
        # The footer is the derive function run over the other footer columns
        if self.footer:
            try:
                return self.derive_func(footer)
            except TypeError:
                # Got None for a value, so return None
                return None
            except DIVISION_BY_ZERO:
                return decimal.Decimal('0.00')
