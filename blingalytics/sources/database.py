"""
The database source provides an interface for querying data from a table in
the database.

.. note::

    The database source requires SQLAlchemy to be installed and connected to
    your database. It also expects your tables to be described using Elixir.
    See :doc:`/install`.

The source intentionally does not do any joins for performance reasons.
Because the reports will often be run over very large data sets, we want to be
sure that running the reports is not prohibitively time-consuming or hogging
the database.

If you want to do produce reports over multiple tables, the best option is
generally to pre-join the tables into one merged reporting table, and then
run the report over that. In "enterprise-ese" this is basically a `star-schema
table`_ in your database with an `ETL`_ process to populate your data into it.
The interwebs have plenty to say about this topic, so we'll leave this issue
in your capable hands.

.. _star-schema table: http://en.wikipedia.org/wiki/Star_schema
.. _ETL: http://en.wikipedia.org/wiki/Extract,_transform,_load

If a whole new reporting database table is too heavy-handed for your use case,
there are a couple of simpler options. Often all you want is to pull in a bit
of data from another table, which you can do with the :class:`Lookup` column.
You can also use the :doc:`/sources/merge` to combine the results of two or
more reports over two or more database tables.

When using columns from the database source, you'll be expected to provide an
extra report attribute to specify which table to pull the data from:

* ``database_entity``: This report attribute specifies the database table to
  query. It should be specified as a dotted-path string pointing to an Elixir
  ``Entity`` subclass. For example::
  
      database_entity = 'model.reporting.ReportInfluencer'

"""

from collections import defaultdict
import heapq
import itertools

import elixir
from sqlalchemy.sql import func

from blingalytics import sources
from blingalytics.utils.collections import OrderedDict


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
        categorized = OrderedDict()
        for name, column in self._columns:
            if isinstance(column, Lookup):
                category = (column.pk_attr, column.pk_column)
                columns = categorized.get(category, [])
                columns.append((name, column))
                categorized[category] = columns
        return categorized

    def _perform_lookups(self, staged_rows):
        # Performs lookup queries for each table for the staged rows and
        # returns the rows with lookups added
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
            q = elixir.session.query(pk_attr, *columns)
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
            q = elixir.session.query(*query_columns)
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
    Filters the database query or queries for this report.

    This filter expects one positional argument, a function defining the
    filter operation. This function will be passed as its first argument the
    ``Entity`` object. If a widget is defined for this filter, the function
    will also be passed a second argument, which is the user input value. The
    function should return a filtering term that can be used to filter a query
    on that entity. Or, based on the user input, the filter function can
    return ``None`` to indicate that no filtering should be done.

    More specifically, the returned object should be a
    ``sqlalchemy.sql.expression._BinaryExpression`` object. You will generally
    build these in a lambda like so::

        database.QueryFilter(lambda entity: entity.is_active == True)

    Or, with a user input widget::

        database.QueryFilter(
            lambda entity, user_input: entity.user_id.in_(user_input),
            widget=Autocomplete(multiple=True))

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
    A transform allows you to alter a database column for every report column
    or other filter that needs to access it. For example, this can be used to
    provide a timezone offset option that shifts all date and time columns by
    a certain number of hours.

    This filter expects one positional argument, a function defining the
    transform operation. This function will be passed the Elixir column object
    as its first argument. If a widget is defined for this filter, the
    function will also be passed a second argument, which is the user input
    value. The function should return the altered column object.

    This filter **requires** the columns keyword argument, which should be a
    list of strings referring to the columns this transform will be applied
    to.

    For example, if you have a database column with the number of hours since
    the epoch and want to transform it to the number of days since the epoch,
    with a given number of hours offset for timezone, you can use::

        database.ColumnTransform(
            lambda column, user_input: column.op('+')(user_input).op('/')(24),
            columns=['purchase_time', 'user_last_login_time'],
            widget=widgets.Select(choices=TIMEZONE_CHOICES))
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
    This column allows you to "cheat" on the no-joins rule and look up a value
    from an arbitrary database table by primary key.

    This column expects several positional arguments to specify how to do the
    lookup:

    * The Elixir ``Entity`` object to look up from, specified as a
      dotted-string reference.
    * A string specifying the column attribute on the ``Entity`` you want to
      look up.
    * The name of the column in the report which is the primary key to use for
      the lookup in this other table.

    The primary key name on the lookup table is assumed to be 'id'. If it's
    different, you can use the keyword argument:
    
    * ``pk_attr``: The name of the primary key column in the lookup database
      table. Defaults to ``'id'``.

    For example::

        database.Lookup('project.models.Publisher', 'name', 'publisher_id',
            format=formats.String)

    Because the lookups are only done by primary key and are bulked up into
    just a few operations, this isn't as taxing on the database as it could
    be. But doing a lot of lookups on large datasets can get pretty
    resource-intensive, so it's best to be judicious.
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
    Performs a group-by operation on the given database column. It takes one
    positional argument: a string specifying the column to group by. There is
    also an optional keyword argument:

    * ``include_null``: Whether the database column you're grouping on should
      filter out or include the null group. Defaults to ``False``, which will
      not include the null group.

    Any group-by columns should generally be listed in your report's keys.
    You are free to use more than one of these in your report, which will be
    treated as a multi-group-by operation in the database.

    This column does not compute or output a footer.
    """
    def __init__(self, entity_column, include_null=False, **kwargs):
        self.include_null = include_null
        super(GroupBy, self).__init__(entity_column, **kwargs)

    def get_query_column(self, entity):
        if isinstance(self.entity_column, basestring):
            return getattr(entity, self.entity_column)
        return self.entity_column

    def get_query_modifiers(self, entity):
        # If we're removing the null grouping, filter it out
        if not self.include_null:
            column = self.get_query_column(entity)
            return [lambda q: q.filter(column != None)]
        return []

    def get_query_group_bys(self, entity):
        return [self.get_query_column(entity)]

    def increment_footer(self, total, cell):
        # Never return a footer
        return None

    def finalize_footer(self, total, footer):
        # Never return a footer
        return None

class Sum(DatabaseColumn):
    """
    Performs a database sum aggregation. The first argument should be a string
    specifying the database column to sum.
    """
    def get_query_column(self, entity):
        if isinstance(self.entity_column, DatabaseColumn):
            return func.sum(self.entity_column.get_query_column(entity))

        return func.sum(getattr(entity, self.entity_column))

class Count(DatabaseColumn):
    """
    Performs a database count aggregation. The first argument should be a
    string specifying the database column to count on. This also accepts one
    extra keyword argument:

    * ``distinct``: Whether to perform a distinct count or not. Defaults to
      ``False``.
    """
    def __init__(self, entity_column, distinct=False, **kwargs):
        self._distinct = bool(distinct)
        super(Count, self).__init__(entity_column, **kwargs)

    def get_query_column(self, entity):
        column = getattr(entity, self.entity_column)
        if self._distinct:
            column = column.distinct()
        return func.count(column)

class First(DatabaseColumn):
    """
    .. note::

        Using this column requires that your database have a ``first``
        aggregation function. In many databases, you will have to add this
        aggregate yourself. For example, here is a
        `PostgreSQL implementation`_.

    .. _PostgreSQL implementation: http://wiki.postgresql.org/wiki/First_(aggregate)

    Performs a database first aggregation to return the first value found. The
    first argument should be a string specifying the database column.
    """
    def get_query_column(self, entity):
        return func.first(getattr(entity, self.entity_column))

class BoolAnd(DatabaseColumn):
    """
    .. note::

        Using this column requires that your database have a ``bool_and``
        aggregation function.

    Performs a boolean-and aggregation. This aggregates to true if *all* the
    aggregated values are true; otherwise, it will aggregate to false. The
    first argument should be a string specifying the database column to
    aggregate on.
    """
    def get_query_column(self, entity):
        return func.bool_and(getattr(entity, self.entity_column))

class BoolOr(DatabaseColumn):
    """
    .. note::

        Using this column requires that your database have a ``bool_or``
        aggregation function.

    Performs a boolean-or aggregation. This aggregates to true if *any* of the
    aggregated values are true; otherwise, it will aggregate to false. The
    first argument should be a string specifying the database column to
    aggregate on.
    """
    def get_query_column(self, entity):
        return func.bool_or(getattr(entity, self.entity_column))

class ArrayAgg(DatabaseColumn):
    """
    .. note::

        Using this column requires that your database have an ``array_agg``
        aggregation function.

    Performs an array aggregation. This essentially compiles a list of all
    the values in all the rows being aggregated. The first argument should be
    a string specifying the database column to aggregate.
    """
    def get_query_column(self, entity):
        return func.array_agg(getattr(entity, self.entity_column))

class Greatest(DatabaseColumn):
    """
    .. note::

        Using this column requires that your database have a ``greatest``
        function.

    Picks the greatest value out of the supplied list of enity column names.
    Please see your database's docs for the ``greatest`` function's handling of
    nulls, etc.
    """
    def __init__(self, *args, **kwargs):
        assert len(args) >= 2, 'You must supply at least 2 column names to be compared.'
        self.entity_columns = args
        super(DatabaseColumn, self).__init__(**kwargs)

    def get_query_column(self, entity):
        return func.greatest(*(getattr(entity, c) for c in self.entity_columns))

class Least(DatabaseColumn):
    """
    .. note::

        Using this column requires that your database have a ``least``
        function.

    Picks the least value out of the supplied list of enity column names.
    Please see your database's docs for the ``least`` function's handling of
    nulls, etc.
    """
    def __init__(self, *args, **kwargs):
        assert len(args) >= 2, 'You must supply at least 2 column names to be compared.'
        self.entity_columns = args
        super(DatabaseColumn, self).__init__(**kwargs)

    def get_query_column(self, entity):
        return func.least(*(getattr(entity, c) for c in self.entity_columns))

class TableKeyRange(sources.KeyRange):
    """
    This key range ensures that there is a key for every row in the given
    database table. This is primarily useful to ensure that you get every row
    ID from an external table in your report.

    This key range takes one positional argument, a dotted-string reference to
    the ``Entity`` to pull from. It also takes two optional keyword arguments:
    
    * ``pk_column``: The column name for the primary key to use from the
      table. Defaults to ``'id'``.
    * ``filters``: Either a single filter or a list of filters. These filters
      will be applied when pulling the keys from this database table.
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
        q = elixir.session.query(self.pk_column)

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
