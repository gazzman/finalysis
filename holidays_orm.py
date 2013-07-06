#!/usr/bin/python
from sqlalchemy import Table
from sqlalchemy import Column, Date, String

def gen_table(tablename, metadata, schema=None):
    '''Generate an option chain table

    tablename   -- the name of the table
    metadata    -- the sqlalchemy MetaData() object
    schema      -- the schema in which the table resides
    '''

    return Table(tablename, metadata,
                 Column('holiday', String, index=True),
                 Column('date', Date, primary_key=True),
                 schema=schema)
