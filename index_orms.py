#!/usr/bin/python
from sqlalchemy import Table
from sqlalchemy import Column, Date, String
from sqlalchemy.dialects.postgresql import VARCHAR

def gen_table(tablename, metadata, schema=None):
    '''Generate an index components table

    tablename   -- the name of the table
    metadata    -- the sqlalchemy MetaData() object
    schema      -- the schema in which the table resides
    '''

    return Table(tablename, metadata,
                 Column('symbol', VARCHAR(21), index=True, primary_key=True),
                 Column('date', Date, index=True),
                 Column('url', String),
                 schema=schema)
