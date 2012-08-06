#!/usr/bin/python
"""tickerdata: Pull ticker data from web"""
__version__ = ".02"
__author__ = "gazzman"
__copyright__ = "(C) 2012 gazzman GNU GPL 3."
__contributors__ = []

from datetime import datetime, timedelta
from StringIO import StringIO
from time import sleep
import argparse
import re
import sys
import urllib2

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Date, Float

def pull_from_yahoo(tickers, db='mobil_db', schema='yahoo_tickers', 
                    totext=False):
    """pull_from_yahoo:

    This script pulls historical ticker data from yahoo and stores 
    it in a postgresql database.
    
    Requires a list of tickers to pull data from, the name of the db,
    and the schema where the ticker tables should be created

    """
    # URL prep
    base_url = 'http://ichart.finance.yahoo.com/table.csv?s='
    xtra = '&g=d&ignore=.csv'

    ua_string = 'Mozilla/5.0 (X11; Linux x86_64; rv:10.0.1)'
    ua_string += ' Gecko/20120225 Firefox/10.0.1'
    reqheader = {'User-Agent' : ua_string}

    # Date prep
    from_date_format = '&a=%(fmonth)s&b=%(fday)s&c=%(fyear)s'
    to_date_format = '&d=%(tmonth)s&e=%(tday)s&f=%(tyear)s'

    ddic = {}
    fdate = datetime.now() - timedelta(days=3)
    ddic['tmonth'] = str(fdate.month - 1)
    ddic['tday'] = str(fdate.day)
    ddic['tyear'] = str(fdate.year)
    ddic['fmonth'] = str(0)
    ddic['fday'] = str(1)
    ddic['fyear'] = str(1900)

    from_date = from_date_format % ddic
    to_date = to_date_format  % ddic

    if not totext:
        # SQL prep
        engine = create_engine('postgresql+psycopg2:///' + db)
        metadata = MetaData(bind=engine, reflect=True)
        conn = engine.raw_connection()
        cur = conn.cursor()

    for ticker in tickers:
        # Get the data
        urlticker = ticker.strip().upper()
        url = base_url + urlticker + from_date + to_date + xtra
        ticker = urlticker.replace('%5E','')
        print >> sys.stderr, 'Starting ' + ticker + '...',
        req = urllib2.Request(url, headers=reqheader)
        opened = False
        while not opened:
            try:
                page = urllib2.urlopen(req)
                opened = True
            except urllib2.HTTPError as err:
                if re.match('HTTP Error 404', str(err)):
                    print >> sys.stderr, '...404 problem...waiting 5 sec...',
                    sleep(5)
                else:
                    raise err
        headers = page.readline().strip().lower().replace(' ','_').split(',')

        # Write as csv to memory buffer
        mempage = StringIO()
        if totext:
            for line in page: mempage.write(','.join([ticker, line]))
        else:
            for line in page: mempage.write(line)
        mempage.seek(0)

        if totext:
            # Write to csv file
            headers = ','.join(headers)
            with open(ticker + '.csv', 'w') as f:
                f.write('ticker,' + headers + '\n')
                f.write(mempage.read() + '\n')
        else:
            # Upload to database
            tickertable = Table(ticker.lower(), metadata, schema=schema)
            tickertable.drop(checkfirst=True)
            tickertable.append_column(Column('date', Date, primary_key=True))
            for header in headers[1:]:
                tickertable.append_column(Column(header, Float(24)))
            tickertable.create()
            cur.copy_from(mempage, ticker.lower(), sep=',', columns=headers)
            conn.commit()
        print >> sys.stderr, 'Done!'
        
if __name__ == "__main__":
    p = argparse.ArgumentParser(description='Pull ticker data from web')
    p.add_argument('tickerfile', type=str, 
                   help='a text file of ticker sybols, one per line')
    p.add_argument('-v', '--version', action='version', 
                   version='%(prog)s ' + __version__)

    g1 = p.add_argument_group()        
    g1 = p.add_argument_group('Store results in postgres database')
    g1.add_argument('-d', metavar='db', default='mobil_db', 
                    dest='db', help='name of db in which to store the data')
    g1.add_argument('-s', metavar='schema', default='yahoo_tickers', 
                    dest='schema',
                    help='name of schema in which to store data tables') 

    g2 = p.add_argument_group('Write to file(s)')    
    g2.add_argument('-f', dest='totext', action='store_true', 
                    help='write data to \'ticker_symbol\'.csv file(s)')
    args = p.parse_args()

    with open(args.tickerfile, 'r') as f:
        tickers = f.read().strip().split('\n')

    pull_from_yahoo(tickers, db=args.db, schema=args.schema, 
                    totext=args.totext)
