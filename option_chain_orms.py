#!/usr/bin/python
from sqlalchemy import Table
from sqlalchemy import Boolean, Column, Date, DateTime
from sqlalchemy.dialects.postgresql import NUMERIC, VARCHAR

BAR = ('open', 'high', 'low', 'close', 'hasgaps')
SHOWTUPLE = [('trades', BAR + ('volume', 'wap', 'count')),
             ('bid', BAR),
             ('ask', BAR)]
SHOWCOLS = ['%s_%s' % (show[0], bar) for show in SHOWTUPLE for bar in show[1]]


def gen_table(tablename, metadata, links=16, right='b', schema=None):
    '''Generate an option chain table

    tablename   -- the name of the table
    metadata    -- the sqlalchemy MetaData() object
    links       -- the total number of strike prices in chain
    right       -- 'c'all, 'p'ut, or 'b'oth
    schema      -- the schema in which the table resides
    '''
    assert links > 0
    assert type(links) == int
    assert right in ['c', 'p', 'b']

    if right == 'b': rights = ['c', 'p']
    else: rights = [right]    

#    undercols = [Column(sh, NUMERIC(19, 4)) for sh in SHOWCOLS]
#    linkcols = [Column('%s_%02i_%s' % (cp, n, sh), NUMERIC(19, 4)) 
#                for cp in rights  for n in range(0, links+1) 
#                for sh in SHOWCOLS]
    undercols = []
    for sh in SHOWCOLS:
        if 'hasgaps' not in sh: undercols += [Column(sh, NUMERIC(19, 4))]
        else: undercols += [Column(sh, Boolean)]
    linkcols = []
    for cp in rights:
        for n in range(0, links):
            for sh in SHOWCOLS:
                c = '%s_%02i_%s' % (cp, n, sh)
                if 'hasgaps' not in sh: linkcols += [Column(c, NUMERIC(19, 4))]
                else: linkcols += [Column(c, Boolean)]

    datacols = undercols + linkcols
    return Table(tablename, metadata,
                 Column('underlying', VARCHAR(21), index=True,
                        primary_key=True),
                 Column('osi_underlying', VARCHAR(21), index=True, 
                        primary_key=True),
                 Column('timestamp', DateTime(timezone=True), index=True, 
                        primary_key=True),
                 Column('strike_start', NUMERIC(19,4), index=True, 
                        primary_key=True),
                 Column('strike_interval', NUMERIC(19,4), index=True, 
                        primary_key=True),
                 Column('expiry', Date, index=True, primary_key=True),
                 *datacols,
                 schema=schema)
