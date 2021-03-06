#!/usr/bin/python
import argparse
import csv
import re
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.ext.declarative import declarative_base

from finalysis.research_orms import (Base,
                                     Ticker as tickers,
                                     AssetAllocation as asset_allocation,
                                     CountryAllocation as country_allocation,
                                     Equity as equity,
                                     FixedIncome as fixed_income,
                                     Fund as fund,
                                     Holdings as holdings,
                                     MktCapAllocation as mkt_cap_allocation,
                                     RegionAllocation as region_allocation,
                                     SectorAllocation as sector_allocation,
                                     SCHEMA)

def clean_field(field):
    field = field.lower()
    field = field.replace('*', '')
    field = field.replace('&', 'and')
    field = field.replace('(', '')
    field = field.replace(')', '')
    field = field.replace('\'', '')
    field = field.replace('-', ' ')
    field = field.replace('/', ' ')
    field = field.replace('%', 'pct')
    field = field.replace(' ', '_')
    field = field.replace('avg.', 'avg')
    field = field.replace('_help', '')
    if re.search('\.\d', field): field = field.replace('.', '._')
    table, dot, field = field.partition('.')
    return ''.join([table, dot, field[:63]])

def clean_data(data):
    if not data: return None
    data = data.strip()
    if data == '' or data == 'N/A' or '--' in data: return None
    if data[-2:] == 'Yr': data = data.replace('Yr', '')
    if '%' in data:
        data = data.replace('%', '')
        return float(data)
    if '$' in data:
        data = data.replace('$', '')
    if data[0] in ['-'] + [str(x) for x in range(0, 10)]: data = data.replace(',', '')
    if   data[-1] == 'K': data = float(data[:-1])*1000
    elif data[-1] == 'M': data = float(data[:-1])*1000000
    elif data[-1] == 'B': data = float(data[:-1])*1000000000
    return data

def add_merge(session, orm):
    try:
        session.add(orm)
        session.commit()
        return 'Added'
    except IntegrityError as err:
        session.rollback()
        if 'duplicate key' in str(err):
            session.merge(orm)
            session.commit()
            return 'Merged'
        else: raise(err)           

if __name__ == "__main__":
    description = 'A utility for storing schwab_data.rb data in a db.'

    p = argparse.ArgumentParser(description=description)
    p.add_argument('datafile', type=str)
    p.add_argument('db_name', type=str)
    p.add_argument('--db_host', type=str, default='localhost')
    p.add_argument('--db_port', type=int, default='5432')
    args = p.parse_args()

    datafile = args.datafile
    dburl = 'postgresql+psycopg2://%s:%i/%s' % (args.db_host, 
                                                args.db_port,
                                                args.db_name)
    engine = create_engine(dburl)
    print >> sys.stderr, "Ensuring schema '%s' exists" % SCHEMA
    try: engine.execute(CreateSchema(SCHEMA))
    except ProgrammingError: pass

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # An explicit entry for the CASH symbol
    add_merge(session, tickers(**{'ticker': 'USD', 
                                  'type': 'CASH', 
                                  'description': 'Cash Money'}))
    add_merge(session, asset_allocation(**{'ticker': 'USD', 
                                           'date': '1900-01-01', 
                                           'pct_long_cash': 100}))

    c = csv.DictReader(open(datafile, 'r'))
    c.fieldnames = [clean_field(x) for x in c.fieldnames]
    for row in c:
        tables = {}
        for key in row:
            table, field = key.split('.')
            try: data = clean_data(row[key])
            except ValueError: data = row[key]
            try: tables[table][field] = data
            except KeyError: tables[table] = {'ticker': row['tickers.ticker'],
                                              field: data}
        add_merge(session, tickers(**tables['tickers']))
        del tables['tickers']
        for table in tables:
            if tables[table]['date']:
                orm = locals()[table](**tables[table])
                act = add_merge(session, orm)
                msg = '%s data for %s in %s.%s' % (act, row['tickers.ticker'], 
                                                   SCHEMA, table)
                print >> sys.stderr, msg
