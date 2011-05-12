"""
The static source provides a column for inserting static data. It's incredibly
simple and not terribly useful, but it can come in handy from time to time.
For example, if you want to fill a column with a 'coming soon...' message, you
could use a static column.
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
    Returns a given value for each row in the report. In addition to the
    standard column options, it takes one positional argument, which is the
    static value to return for every row.
    """
    def __init__(self, value, **kwargs):
        self.value = value
        super(Value, self).__init__(**kwargs)
