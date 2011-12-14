"""
The derived source allows you to perform arbitrary operations using the data
returned in columns from other sources.

For example, if you have a report with a column for gross revenue and a column
for net revenue that are both pulled from the database, you could have a
derived column to display the gross margin by performing the operation
``(net revenue / gross revenue * 100)``.
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
    A column that derives its value from other columns in the row. In
    addition to the standard column options, this takes one positional
    argument: the function used to derive the value.

    The function you provide will be passed one argument: the row, as pulled
    from other data sources but before the ``post_process`` step. The row is
    a dict with the column names as keys. Your function should return just the
    derived value for this column in the row. The function is often provided
    as a lambda, but more complex functions can be defined wherever you like.

    Continuing the example from above::

        derived.Value(lambda row: row['net'] / row['gross'] * Decimal('100.00'))

    By default, the footer for this column performs the same operation over
    the appropriate footer columns. This is generally the footer you want for
    a derived column, as opposed to simply summing or averaging the values in
    the column. If one of the columns involved in the derive function does not
    return a footer, this will return a total.
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

    def finalize_footer(self, total, footer):
        # The footer is the derive function run over the other footer columns
        if self.footer:
            try:
                return self.derive_func(footer)
            except TypeError:
                # Got None for a value, so return None
                return total
            except DIVISION_BY_ZERO:
                return decimal.Decimal('0.00')

class Aggregate(DerivedColumn):
    """
    A column that outputs a running total of another column.

    Example usage::

        derived.Aggregate(lambda row: row['subs'], format=formats.Integer)

    This column does not compute or output a footer.
    """
    def __init__(self, derive_func, **kwargs):
        self.total = 0
        self.derive_func = derive_func
        super(Aggregate, self).__init__(**kwargs)
        # Never return a footer
        self.footer = False

    def get_derived_value(self, row):
        result = self.derive_func(row)
        if result:
            self.total += result
        return self.total

    def finalize(self):
        self.total = 0
