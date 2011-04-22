from datetime import date

from blingalytics import base, formats, sources
from blingalytics.sources import database, derived, static


class SuperBasicReport(base.Report):
    filters = []
    keys = ('id', sources.EpochKeyRange(date(2011, 1, 1), date(2011, 1, 3)))
    columns = [
        ('id', static.Value(1, format=formats.Integer)),
    ]
    default_sort = ('id', 'desc')

class BasicDatabaseReport(base.Report):
    database_entity = 'tests.entities.AllTheData'
    filters = []
    keys = ('user_id', sources.SourceKeyRange)
    columns = [
        ('user_id', database.GroupBy('user_id', format=formats.Integer(label='User ID', grouping=False))),
        ('user_is_active', database.First('user_is_active', format=formats.Boolean(label='Active?'))),
        ('num_widgets', database.Count('widget_id', distinct=True, format=formats.Integer(label='Widgets'))),
        ('_sum_widget_price', database.Sum('widget_price')),
        ('average_widget_price', derived.Value(lambda row: row['_sum_widget_price'] / row['num_widgets'], format=formats.Bling)),
    ]
    default_sort = ('average_widget_price', 'desc')
