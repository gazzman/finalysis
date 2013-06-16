#!/usr/bin/python
from sqlalchemy import Table
from sqlalchemy import Column, DateTime
from sqlalchemy.dialects.postgresql import NUMERIC, VARCHAR

def gen_table(tablename, metadata, links=16, right='b', schema=None):
    '''Generate an option chain table
    
    tablename   -- the name of the table
    metadata    -- the sqlalchemy MetaData() object
    links       -- the total number of strike prices in chain
    right       -- 'c'all, 'p'ut, or 'b'oth
    schema      -- the schema in which the table resides
    '''
    assert links <= 16
    assert links % 1 == 0
    assert right in ['c', 'p', 'b']

    if right == 'b': rights = ['c', 'p']
    else: rights = [right]    

    shows = ['%s_%s' % (sh, ohlc) for sh in ['trades', 'bid', 'ask'] 
                                  for ohlc in ['open', 'high', 'low', 'close']]
    shows += ['volume', 'wap', 'count', 'hasGaps']
    undercols = [Column(sh, NUMERIC(19, 4)) for sh in shows]
    linkcols = [Column('%s_%s_%s' % (cp, n, show), NUMERIC(19, 4)) 
                for cp in rights  for n in range(0, links+1) for show in shows]
    datacols = undercols + linkcols
    return Table(tablename, metadata,
                 Column('underlying', VARCHAR(21), index=True, 
                        primary_key=True),
                 Column('timestamp', DateTime(timezone=True), index=True, 
                        primary_key=True),
                 Column('osi_underlying', VARCHAR(21)),
                 Column('strike_start', NUMERIC(19,4), index=True, 
                        primary_key=True),
                 Column('strike_interval', NUMERIC(19,4), index=True, 
                        primary_key=True),
                 *datacols,
                 schema=schema)
