#!/usr/bin/python
from datetime import datetime
import argparse
import csv
import re
import sys

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema, DropSchema
import xlrd

from finalysis.index_orms import gen_table

COLNAME_TRANS = {'symbol': 'ticker',
                 'constituent': 'name'}
STARTPATTERN = ['Constituent', 'Symbol']
ENDPATTERN = ['', '']


if __name__ == "__main__":
    description = 'A utility for storing pull_dow_index_component.rb data in a db.'

    p = argparse.ArgumentParser(description=description)
    p.add_argument('filename', type=str)
    p.add_argument('db_name', type=str)
    p.add_argument('tablename', type=str)
    p.add_argument('--schema', type=str, default='public')
    p.add_argument('--db_host', type=str, default='localhost')
    p.add_argument('--db_port', type=int, default='5432')
    args = p.parse_args()

    # Establish connection to db
    dburl = 'postgresql+psycopg2://%s:%i/%s' % (args.db_host, 
                                                args.db_port,
                                                args.db_name)
    engine = create_engine(dburl)
    conn = engine.connect()
    print >> sys.stderr, "Connected to db %s" % args.db_name

    # Create table and schema if necessary
    if args.schema:
        try: engine.execute(CreateSchema(args.schema))
        except ProgrammingError: pass
    metadata = MetaData(engine)
    table = gen_table(args.tablename, metadata, schema=args.schema)
    metadata.create_all()
    print >> sys.stderr, "Preparing to write to table %s.%s" % (args.schema,
                                                                args.tablename)

    # Parse the xls data into a list of dicts
    wb = xlrd.open_workbook(args.filename)
    sh = wb.sheet_by_index(0)
    data = [sh.row_values(x) for x in xrange(sh.nrows)]
    sidx = data.index(STARTPATTERN)
    data = data[sidx:]
    eidx = data.index(ENDPATTERN)
    data = data[:eidx]
    headers, body = [h.lower() for h in data[0]], data[1:]
    headers = [COLNAME_TRANS[header] for header in headers]
    rows = [dict(zip(headers, row)) for row in body]

    # Inserting (or updating) into db
    date = datetime.now().date().strftime("%Y-%m-%d")
    for row in rows:
        row['ticker'] = row['ticker'].replace('.', '/')
        try:
            conn.execute(table.insert(), date=date, **row)
            print >> sys.stderr, "Writing %s" % row['ticker']
        except IntegrityError as err:
            if 'duplicate key' in str(err): 
                conn.execute(table.update().where(table.c.ticker==row['ticker']),
                             date=date, **row)
                print >> sys.stderr, "Updated %s" % row['ticker']
            else: raise(err)
