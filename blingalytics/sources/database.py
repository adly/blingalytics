"""
Database data source implementation.

This source provides an interface for querying data from a table in the
database. It intentionally does not do any joins for performance reasons.
Because the reports will often be run over very large data sets, we want to be
sure that running the reports is not prohibitively time-consuming or hogging
the database. 

If you want to do reporting over multiple tables, one option is to pre-join
the tables into one merged reporting table, and then run the report over that.
Another quick-fix solution is to use merged reports to run queries over
multiple tables.

Report Attributes:

* database_entity: This report attribute specifies the database table to
  query. It should be specified as a dotted-path string pointing to an Elixir
  Entity class. For example:
  
  database_entity = 'model.reporting.ReportInfluencer'

Key Ranges:
* database.TableKeyRange: Uses the rows from a database table for keys in a
  report.

Columns:

* database.GroupBy: Performs a database group by on the given column. You can
  have more than one in a report if you want to do a multi-group-by. All group
  by columns should also be report keys.
* database.Sum: A database sum over the given column.
* database.Count: A database count over the given column.
* database.First: A database first operation, which returns the first non-null
  value.
* database.BoolAnd: A boolean and database operation.
* database.BoolOr: A boolean or database operation.
* database.ArrayAgg: Aggregates database values into an array.
* database.Lookup: Performs a lookup on another database table by primary key.

Filters: 

* database.QueryFilter: Adds a filter (essentially a SQL "where" clause) to
  the query.
* database.ColumnTransform: Performs an arbitrary transformation on a column
  wherever it is used in the report.
"""

from collections import defaultdict
import heapq
import itertools

from sqlalchemy.sql import func

from logic.blingalytics3 import sources
from util import database


QUERY_LIMIT = 250

class DatabaseSource(sources.Source):
    def __init__(self, report):
        super(DatabaseSource, self).__init__(report)
        self.set_database_entity(report.database_entity)

    def set_database_entity(self, entity):
        # Receive the database entity class from the report definition.
        module, name = entity.rsplit('.', 1)
        module = __import__(module, globals(), locals(), [name])
        self._entity = getattr(module, name)

    @property
    def _query_filters(self):
        # Organize the QueryFilters by the columns they apply to.
        key_columns = set(dict(self._keys).keys())
        filtered_columns = set()
        query_filters = defaultdict(list)

        for name, report_filter in self._filters:
            if isinstance(report_filter, QueryFilter):
                if report_filter.columns:
                    if report_filter.columns & filtered_columns:
                        raise ValueError('You cannot include the same column '
                            'in more than one database filter.')
                    elif report_filter.columns & key_columns:
                        raise ValueError('You cannot filter key columns '
                            'since they are used in every filter query. '
                            'Maybe you could try a report-wide filter.')
                    else:
                        filtered_columns |= report_filter.columns
                query_filters[report_filter.columns].append(report_filter)

        # Determine the list of unfiltered database columns
        # (Exclude lookup columns)
        query_columns = [
            name for name, column in self._columns
            if isinstance(column, DatabaseColumn)
        ]
        unfiltered_columns = frozenset(query_columns) \
            - filtered_columns - key_columns
        if unfiltered_columns:
            query_filters[unfiltered_columns] = []

        return query_filters

    def _lookup_columns(self):
        # Organize the Lookup columns by the name of the column providing its
        # primary key and the entity primary key column.
        categorized = defaultdict(list)
        for name, column in self._columns:
            if isinstance(column, Lookup):
                category = (column.pk_attr, column.pk_column)
                categorized[category].append((name, column))
        return categorized

    def _perform_lookups(self, staged_rows):
        # Performs lookup queries for each table for the staged rows and
        # returns the rows with lookups added
        session = database.get_session()
        for (pk_attr, pk_column), lookups in self._lookup_columns().items():
            # Collect the pk ids from the staged rows
            pk_column_ids = [
                row[pk_column] for key, row in staged_rows
                if pk_column in row
            ]
            if not pk_column_ids:
                continue

            # Collate the lookup columns into name list and lookup_attr list
            names, columns = zip(*lookups)
            columns = map(lambda column: column.lookup_attr, columns)

            # Construct the bulked query
            q = session.query(pk_attr, *columns)
            q = q.filter(pk_attr.in_(pk_column_ids))
            lookup_values = dict(map(
                lambda row: (row[0], dict(zip(names, row[1:]))),
                q.all()))

            # Update the staged rows with the looked-up values
            for key, row in staged_rows:
                looked_up_pk = row.get(pk_column)
                if looked_up_pk:
                    row.update(lookup_values.get(looked_up_pk, {}))

        return staged_rows

    def _column_transforms(self):
        # Organize the ColumnTransforms by the columns they apply to.
        column_transforms = defaultdict(list)
        for name, report_filter in self._filters:
            if isinstance(report_filter, ColumnTransform):
                for column in report_filter.columns:
                    column_transforms[column].append(report_filter)
        return column_transforms

    def _queries(self, clean_inputs):
        # Provides a list of iterators over the required queries, filtered
        # appropriately, and ensures each row is emitted with the proper
        # formatting: ((key), {row})
        key_column_names = map(lambda a: a[0], self._keys)
        entity = EntityProxy(self._entity, self._column_transforms(), clean_inputs)
        session = database.get_session()
        queries = []

        # Create a query object for each set of report filters
        query_filters_by_columns = self._query_filters
        table_wide_filters = query_filters_by_columns.pop(None, [])

        # Ensure we do a query even if we have no non-key columns (odd but possible)
        query_filters_by_columns = query_filters_by_columns.items() or [([], [])]

        for column_names, query_filters in query_filters_by_columns:
            # Column names need to be a list to guarantee consistent ordering
            filter_column_names = key_column_names + list(column_names)
            query_columns = []
            query_modifiers = []
            query_group_bys = []

            # Collect the columns, modifiers, and group-bys
            for name in filter_column_names:
                column = self._columns_dict[name]
                query_columns.append(column.get_query_column(entity))
                query_modifiers += column.get_query_modifiers(entity)
                query_group_bys += column.get_query_group_bys(entity)

            # Construct the query
            q = session.query(*query_columns)
            for query_modifier in query_modifiers:
                q = query_modifier(q)
            for query_filter in itertools.chain(table_wide_filters, query_filters):
                filter_arg = query_filter.get_filter(entity, clean_inputs)
                if filter_arg is not None:
                    q = q.filter(filter_arg)
            q = q.order_by(*query_group_bys)
            q = q.group_by(*query_group_bys)

            # Set up iteration over the query, with formatted rows
            # (using generator here to make a closure for filter_column_names)
            def rows(q, filter_column_names):
                for row in q.yield_per(QUERY_LIMIT):
                    yield dict(zip(filter_column_names, row))
            queries.append(itertools.imap(
                lambda row: (tuple(row[name] for name, _ in self._keys), row),
                rows(q, filter_column_names)
            ))

        return queries

    def get_rows(self, key_rows, clean_inputs):
        # Merge the queries for each filter and do bulk lookups
        current_row = None
        current_key = None
        staged_rows = []
        for key, partial_row in heapq.merge(key_rows, *self._queries(clean_inputs)):
            if current_key and current_key == key:
                # Continue building the current row
                current_row.update(partial_row)
            else:
                if current_key is not None:
                    # Done with the current row, so stage it
                    staged_rows.append((current_key, current_row))
                    if len(staged_rows) >= QUERY_LIMIT:
                        # Do bulk table lookups on staged rows and emit them
                        finalized_rows = self._perform_lookups(staged_rows)
                        for row in finalized_rows:
                            yield row
                        staged_rows = []
                # Start building the next row
                current_key = key
                current_row = partial_row

        # Do any final leftover lookups and emit
        if current_row is not None:
            staged_rows.append((current_key, current_row))
            finalized_rows = self._perform_lookups(staged_rows)
            for row in finalized_rows:
                yield row

class EntityProxy(object):
    """
    Proxy to database entities while applying appropriate column transforms.
    
    Used by the DatabaseSource. Proxies attribute access to the underlying
    database entity while automatically performing column transforms when
    those columns are accessed.
    """
    def __init__(self, entity, transforms, clean_inputs):
        self.entity = entity
        self.transforms = transforms
        self.clean_inputs = clean_inputs

    def __getattr__(self, attr):
        column = getattr(self.entity, attr)
        for transform in self.transforms[attr]:
            column = transform.transform_column(column, self.clean_inputs)
        return column

class QueryFilter(sources.Filter):
    """
    Provides a filter for the database query.
    
    This filter expects one positional argument, a function defining the
    filter operation. This function will be passed as its first argument the
    entity object (technically, it's an EntityProxy object). If a widget is
    defined for this filter, the function will also be passed a second
    argument, which is the user input value. The function should return a
    sqlalchemy binary expression suitable to pass to the filter method of a
    query. Or the function can return None to indicate no filtering. For
    example:
    
    database.QueryFilter(
        lambda entity, user_input: entity.publisher_id.in_(user_input),
        widget=AutocompleteWidget(...))
    """
    def __init__(self, filter_func, **kwargs):
        self.filter_func = filter_func
        super(QueryFilter, self).__init__(**kwargs)

    def get_filter(self, entity, clean_inputs):
        # Applies the filter function to the entity to return the filter.
        if self.widget:
            user_input = clean_inputs[self.widget._name]
            return self.filter_func(entity, user_input)
        return self.filter_func(entity)

class ColumnTransform(sources.Filter):
    """
    Ensures a database column is altered for every report column that uses it.
    
    For example, this is useful if you want to provide a timezone offset
    option that shifts all date and time columns by a certain number of hours.
    
    This filter expects one positional argument, a function defining the
    transform operation. This function will be passed the elixir column object
    as its first argument. If a widget is defined for this filter, the
    function will also be passed a second argument, which is the user input
    value. The function should return the altered column object.

    This filter requires the columns keyword argument, which should be a list
    of strings referring to the columns this transform will be applied to.
    
    For example:
    
    database.ColumnTransform(
        lambda column, user_input: column.op('+')(user_input).op('/')(24),
        columns=('campaign_contract_sent_time', 'campaign_ad_created_time'),
        widget=widgets.TimezoneSelect(choices=TIMEZONE_CHOICES))
    """
    def __init__(self, filter_func, **kwargs):
        if not len(kwargs.get('columns', [])):
            raise ValueError('You must specify the columns you want this '
                'transform to be applied to.')
        self.filter_func = filter_func
        super(ColumnTransform, self).__init__(**kwargs)

    def transform_column(self, base_column, clean_inputs):
        # Applies the filter function to the entity to return the column.
        if self.widget:
            user_input = clean_inputs[self.widget._name]
            return self.filter_func(base_column, user_input)
        return self.filter_func(base_column)

class DatabaseColumn(sources.Column):
    """
    Base class for a database report column.
    """
    source = DatabaseSource

    def __init__(self, entity_column, **kwargs):
        self.entity_column = entity_column
        super(DatabaseColumn, self).__init__(**kwargs)

    def get_query_column(self, entity):
        # Returns a list of Entity.columns to query for.
        return None

    def get_query_modifiers(self, entity):
        # Returns a list of functions to modify the query object.
        return []

    def get_query_group_bys(self, entity):
        # Returns a list of group-by Entity.columns for the query.
        return []

class Lookup(sources.Column):
    """
    Looks up a value from an arbitrary database table.
    
    This column expects several positional arguments to specify how to do the
    lookup. First is the Elixir Entity object to look up from, specified as a
    dotted-lookup string. Second is a string for the column attribute on the
    entity to look up. Third is the column name from the report where the
    primary key value can be found.
    
    The primary key name on the lookup table is assumed to be 'id'. If it's
    different, you can pass in the pk_attr keyword argument to specify it.
    
    The lookup operations can be pretty time-consuming on large datasets, so
    please try to be judicious in your use of them.
    
    Example:
    
    database.Lookup(
        'model.publisher.Publisher', 'name', 'publisher_id',
        format=formats.String)
    """
    source = DatabaseSource

    def __init__(self, entity, lookup_attr, pk_column, pk_attr='id', **kwargs):
        super(Lookup, self).__init__(**kwargs)
        module, name = entity.rsplit('.', 1)
        module = __import__(module, globals(), locals(), [name])
        self.entity = getattr(module, name)
        self._lookup_attr = lookup_attr
        self._pk_attr = pk_attr
        self.pk_column = pk_column

    @property
    def lookup_attr(self):
        return getattr(self.entity, self._lookup_attr)

    @property
    def pk_attr(self):
        return getattr(self.entity, self._pk_attr)

class GroupBy(DatabaseColumn):
    """
    Performs a group-by operation in the database.
    
    The first argument should be a string naming the column attribute of the
    database entity. It accepts one keyword argument:
    
    * include_null: Whether the database column you're grouping on should
      filter out or include null values. Default is False.
    
    You can use more than one of these in a report, in which case it will be
    treated as a multi-group-by operation in the database. This column should
    also be listed in the report's keys attribute.
    
    By default, there is no footer for this type of column.
    """
    def __init__(self, *args, **kwargs):
        self.include_null = bool(kwargs.pop('include_null', False))
        super(GroupBy, self).__init__(*args, **kwargs)

    def get_query_column(self, entity):
        return getattr(entity, self.entity_column)

    def get_query_modifiers(self, entity):
        # If we're removing the null grouping, filter it out
        if not self.include_null:
            column = self.get_query_column(entity)
            return [lambda q: q.filter(column != None)]
        return []

    def get_query_group_bys(self, entity):
        return [getattr(entity, self.entity_column)]

    def increment_footer(self, total, cell):
        # Never return a footer
        return None

    def finalize_footer(self, total, footer):
        # Never return a footer
        return None

class Sum(DatabaseColumn):
    """
    Performs a database sum aggregate operation.
    
    The first argument should be a string naming the column attribute of the
    database entity.
    """
    def get_query_column(self, entity):
        return func.sum(getattr(entity, self.entity_column))

class Count(DatabaseColumn):
    """
    Performs a database count aggregate operation.
    
    The first argument should be a string naming the column attribute of the
    database entity. Also accepts an extra keyword argument:
    
    * distinct: Whether the database should perform a distinct count
      operation. Defaults to False.
    """
    def __init__(self, *args, **kwargs):
        self._distinct = bool(kwargs.pop('distinct', False))
        super(Count, self).__init__(*args, **kwargs)

    def get_query_column(self, entity):
        column = getattr(entity, self.entity_column)
        if self._distinct:
            column = column.distinct()
        return func.count(column)

class First(DatabaseColumn):
    """
    Performs a database first aggregation to return the first value found.
    
    The first argument should be a string naming the column attribute of the
    database entity.
    """
    def get_query_column(self, entity):
        return func.first(getattr(entity, self.entity_column))

class BoolAnd(DatabaseColumn):
    """
    Performs a boolean and aggregate operation in the database.
    
    This column will return True if all the aggregated values are true;
    otherwise, it will return False.
    
    The first argument should be a string naming the column attribute of the
    database entity.
    """
    def get_query_column(self, entity):
        return func.bool_and(getattr(entity, self.entity_column))

class BoolOr(DatabaseColumn):
    """
    Performs a boolean or aggregate operation in the database.
    
    This column will return True if any of the aggregated values are true;
    otherwise, it will return False.
    
    The first argument should be a string naming the column attribute of the
    database entity.
    """
    def get_query_column(self, entity):
        return func.bool_or(getattr(entity, self.entity_column))

class ArrayAgg(DatabaseColumn):
    """
    Performs an array aggregation operation in the database.
    
    This essentially compiles a list of all the values in all the rows being
    aggregated.
    
    The first argument should be a string naming the column attribute of the
    database entity.
    """
    def get_query_column(self, entity):
        return func.array_agg(getattr(entity, self.entity_column))

class TableKeyRange(sources.KeyRange):
    """
    Ensures a key for every row in the database table.
    
    This key range takes one positional argument, a dotted-string reference to
    the Entity to pull from. It also takes two optional keyword arguments:
    
    * pk_column: The column name for the primary key to use from the table.
      Optional, defaults to 'id'.
    * filters: Either a single filter or a list of filters. These filters will
      be applied when pulling the keys from this database table.
    """
    def __init__(self, entity, pk_column='id', filters=[]):
        module, name = entity.rsplit('.', 1)
        module = __import__(module, globals(), locals(), [name])
        self.entity = getattr(module, name)
        self._pk_column = pk_column
        if isinstance(filters, sources.Filter):
            self.filters = [filters]
        else:
            self.filters = filters

    @property
    def pk_column(self):
        return getattr(self.entity, self._pk_column)

    def get_row_keys(self, clean_inputs):
        # Query for the primary keys
        session = database.get_session()
        q = session.query(self.pk_column)

        # Apply the filters to the query
        for query_filter in self.filters:
            filter_arg = query_filter.get_filter(self.entity, clean_inputs)
            if filter_arg is not None:
                q = q.filter(filter_arg)
        q = q.order_by(self.pk_column)

        # Return the ids
        return itertools.imap(
            lambda row: row[0],
            q.yield_per(QUERY_LIMIT)
        )
