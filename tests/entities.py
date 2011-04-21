from decimal import Decimal

import elixir
from elixir import Boolean, Entity, Field, Integer, Numeric, using_options


DB_URL = 'postgresql://bling:bling@localhost:5432/bling'
# Aggregate function `first` (http://wiki.postgresql.org/wiki/First_%28aggregate%29)
FIRST_FUNCTION = '''
DROP AGGREGATE IF EXISTS public.first(anyelement);
-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement AS $$
        SELECT CASE WHEN $1 IS NULL THEN $2 ELSE $1 END;
$$ LANGUAGE SQL STABLE;
-- And then wrap an aggregate around it
CREATE AGGREGATE public.first (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);
COMMIT;
'''

def init_db():
    """Perform setup tasks to be able to connect to bling test db."""
    elixir.metadata.bind = DB_URL
    elixir.metadata.bind.echo = False
    elixir.setup_all()
    elixir.session.close()

def init_db_from_scratch():
    """Build the necessary stuff in the db to run."""
    init_db()
    elixir.drop_all()
    elixir.create_all()
    elixir.metadata.bind.execute(FIRST_FUNCTION)
    filler_data()

def filler_data():
    datas = [
        {'user_id': 1, 'user_is_active': True, 'widget_id': 1, 'widget_price': Decimal('1.23')},
        {'user_id': 1, 'user_is_active': True, 'widget_id': 2, 'widget_price': Decimal('2.34')},
        {'user_id': 1, 'user_is_active': True, 'widget_id': 3, 'widget_price': Decimal('3.45')},
        {'user_id': 2, 'user_is_active': False, 'widget_id': 4, 'widget_price': Decimal('50.00')},
    ]
    for data in datas:
        AllTheData(**data)
    elixir.session.commit()

class AllTheData(Entity):
    """Star-schema-style Entity for testing purposes."""
    using_options(tablename='all_the_data')

    user_id = Field(Integer)
    user_is_active = Field(Boolean)
    widget_id = Field(Integer)
    widget_price = Field(Numeric(10, 2))
