"""
Static data source implementation.

This source provides a column for inserting static data. It's incredibly
simple and not terribly useful, but it can come in handy from time to time.
For example, if you want to fill a column with a 'coming soon...' message, you
could use a static column.

Columns:

* static.Value: A static data column.

No filters.
"""

from blingalytics import sources


class StaticSource(sources.Source):
    def post_process(self, row, clean_inputs):
        # Add each static column's value to this row
        for name, column in self._columns:
            row[name] = column.value
        return row

class StaticColumn(sources.Column):
    source = StaticSource

class Value(StaticColumn):
    """
    Static value column.
    
    Takes one positional argument, which is the static value for the column.
    This value will be returned as-is for every row in the report.
    """
    def __init__(self, value, **kwargs):
        self.value = value
        super(Value, self).__init__(**kwargs)
