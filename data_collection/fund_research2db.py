#!/usr/bin/python
import csv
import re
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.ext.declarative import declarative_base

from finalysis.data_collection.research_orms import (Base,
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
    if re.search('\.\d', field): field = field.replace('.', '._')
    return field

def clean_data(data):
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

if __name__ == "__main__":
    datafile = sys.argv[1]
    db_name = sys.argv[2]

    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)
    print >> sys.stderr, 'Dropping schema %s' % SCHEMA
    try: engine.execute(DropSchema(SCHEMA, cascade=True))
    except ProgrammingError: pass
    print >> sys.stderr, 'Recreating schema %s' % SCHEMA
    engine.execute(CreateSchema(SCHEMA))

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    c = csv.DictReader(open(datafile, 'r'))
#    for f in c.fieldnames:
#        print '%s,%s' % (f, clean_field(f))
    c.fieldnames = [clean_field(x) for x in c.fieldnames]
    for row in c:
        print >> sys.stderr, 'Adding data for %s... ' % row['tickers.ticker'],
        tables = {}
        for key in row:
            table, field = key.split('.')
            try: data = clean_data(row[key])
            except ValueError: data = row[key]
            try: tables[table][field] = data
            except KeyError: tables[table] = {'ticker': row['tickers.ticker']}
        session.add(tickers(**tables['tickers']))
        session.add(asset_allocation(**tables['asset_allocation']))
        session.add(country_allocation(**tables['country_allocation']))
        session.add(equity(**tables['equity']))
        session.add(fixed_income(**tables['fixed_income']))
        session.add(fund(**tables['fund']))
        session.add(holdings(**tables['holdings']))
        session.add(mkt_cap_allocation(**tables['mkt_cap_allocation']))
        session.add(region_allocation(**tables['region_allocation']))
        session.add(sector_allocation(**tables['sector_allocation']))
        try: session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err): session.rollback()
            else: raise(err)
        print >> sys.stderr, 'Done!'
