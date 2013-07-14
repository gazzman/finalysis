#!/usr/bin/python
from datetime import datetime, timedelta
import argparse


from sqlalchemy import create_engine, MetaData

from finalysis.holidays_orm import gen_table

#def last_trading_day(date, table, results):
def last_trading_day(date, table, results=[], verbose=False):
    whereclause = "date = '%s'" % date.isoformat()
    r = table.select(whereclause=whereclause).execute()
    if date.weekday() >= 5:
        msg = 'Not a trading day:'
        result = (msg, date.strftime('%A'), date.isoformat())
        results += [result]
        last_trading_day(date - timedelta(days=1), table, results, verbose)
#        last_trading_day(date - timedelta(days=1), table)
    elif r.rowcount >= 1:
        row = dict(zip(r.keys(), r.fetchone()))
        msg = 'Not a trading day:'
        result = (msg, row['holiday'], row['date'].isoformat())
        results += [result]
        last_trading_day(date - timedelta(days=1), table, results, verbose)
#        last_trading_day(date - timedelta(days=1), table)
    else:
        msg = 'Most recent trading day:'
        result = (msg, date.strftime('%A'), date.isoformat())
        results += [result]
    if verbose: print '%s %s, %s' % result
    return results

if __name__ == '__main__':
    description = 'A utility for ascertaining the most recent trading day.'

    def_host = 'localhost'
    def_port = 5432
    def_schema = 'public'
    def_tablename = 'holidays'    

    p = argparse.ArgumentParser(description=description)
    p.add_argument('db_name', type=str)
    p.add_argument('--fromdate', type=str, 
                   help='Format is %%Y-%%m-%%d. Default is today.')
    p.add_argument('--host', type=str, default=def_host, 
                   help='Default is %s' % def_host)
    p.add_argument('--port', type=int, default=def_port, 
                   help='Default is %s' % def_port)
    p.add_argument('--schema', type=str, default=def_schema, 
                   help='Default is %s' % def_schema)
    p.add_argument('--tablename', type=str, default=def_tablename, 
                   help='Default is %s' % def_tablename)
    args = p.parse_args()

    dburl = 'postgresql+psycopg2://%s:%i/%s' % (args.host, 
                                                args.port, 
                                                args.db_name)
    engine = create_engine(dburl)
    metadata = MetaData(bind=engine)
    table = gen_table(args.tablename, metadata, args.schema)
    if args.fromdate: date = datetime.strptime(args.fromdate, '%Y-%m-%d').date()
    else: date = datetime.now().date()

    results = last_trading_day(date, table, verbose=True)
