#!/usr/bin/python
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
from datetime import datetime
import argparse
import csv
import re
import sys

from pytz import timezone
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import Boolean, Column, DateTime
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.dialects.postgresql import NUMERIC, VARCHAR

def add_timezone(date, time, locale='US/Eastern', fmt='%Y%m%d %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return ' '.join([dt.date().isoformat(), dt.time().isoformat(), tzone])

def parse_barline(line):
    line_list = line.split()
    (date, time), bar = line_list[4:6], line_list[6:]
    timestamp = add_timezone(date, time)
    bar = dict([x.split('=') for x in bar])
    return timestamp, bar

def bar_to_db(conn, table, symbol, timestamp, bar):
    try:
        conn.execute(table.insert(), ticker=symbol, timestamp=timestamp, **bar)
    except IntegrityError as err:
        if 'duplicate key' in str(err): pass
        else: raise(err)

def gen_table(tablename, metadata, schema=None):
    table = Table(tablename, metadata,
                  Column('ticker', VARCHAR(21), index=True, primary_key=True),
                  Column('timestamp', DateTime(timezone=True), index=True, 
                         primary_key=True),
                  Column('open', NUMERIC(19,4)),
                  Column('high', NUMERIC(19,4)),
                  Column('low', NUMERIC(19,4)),
                  Column('close', NUMERIC(19,4)),
                  Column('volume', NUMERIC(19,4)),
                  Column('count', NUMERIC(19,4)),
                  Column('wap', NUMERIC(19,4)),
                  Column('hasgaps', Boolean),
                  schema=schema)
    return table

def parse_fname_list(fname_list):
    datafile = ' '.join(fname_list)
    show, bar_size, symbol = datafile.split('_')
    tablename = '_'.join([show.lower(), bar_size.replace(' ', '_')])
    symbol = symbol.replace('.txt', '')
    return datafile, tablename, symbol

if __name__ == "__main__":
    description = 'A utility for storing IB bars in a database.'
    datafile_help = 'text file of historical IB data bars'
    db_help = 'the name of a postgresql database'
    schema_help = 'an optional database schema'
    host_help = 'the host on which the db lives'

    p = argparse.ArgumentParser(description=description)
    p.add_argument('datafile', type=str, help=datafile_help, nargs='+')
    p.add_argument('database', help=db_help)
    p.add_argument('--schema', help=schema_help)
    p.add_argument('--host', default='', help=host_help)
    p.add_argument('-v', '--version', action='version', 
                   version='%(prog)s ' + __version__)
    args = p.parse_args()
    datafile, tablename, symbol = parse_fname_list(args.datafile)

    # Establish connection to db
    dburl = 'postgresql+psycopg2://%s/%s' % (args.host, args.database)
    engine = create_engine(dburl)
    conn = engine.connect()
    print >> sys.stderr, "Connected to db %s" % args.database

    # Create schema and table if necessary
    if args.schema:
        try: engine.execute(CreateSchema(args.schema))
        except ProgrammingError: pass
    metadata = MetaData(engine)
    table = gen_table(tablename, metadata, schema=args.schema)
    metadata.create_all()
    print >> sys.stderr, "Preparing to write to table %s.%s" % (args.schema,
                                                                tablename)

    # Write data to db
    with open(datafile, 'r') as f:
        for line in f:
            try:
                timestamp, bar = parse_barline(line.lower())
                bar_to_db(conn, table, symbol, timestamp, bar)
            except ValueError:
                pass
    print >> sys.stderr, "Data written."
