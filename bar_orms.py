#!/usr/bin/python
from sqlalchemy import Table
from sqlalchemy import Boolean, Column, DateTime
from sqlalchemy.dialects.postgresql import NUMERIC, VARCHAR

BAR = ('open', 'high', 'low', 'close', 'hasgaps', 'volume', 'wap', 'count')

def gen_table(tablename, metadata, schema=None):
    '''Generate an option chain table

    tablename   -- the name of the table
    metadata    -- the sqlalchemy MetaData() object
    schema      -- the schema in which the table resides
    '''

    cols = []
    for b in BAR:
        if 'hasgaps' not in b: cols += [Column(b, NUMERIC(19, 4))]
        else: cols += [Column(b, Boolean)]

    return Table(tablename, metadata,
                 Column('symbol', VARCHAR(21), index=True,
                        primary_key=True),
                 Column('timestamp', DateTime(timezone=True), index=True, 
                        primary_key=True),
                 *cols,
                 schema=schema)
