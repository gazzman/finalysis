#!/usr/bin/python
"""tickerdata: Script to pull ticker data off of the web.

Usage:

    tickerdata.py tickerfile [ [-d db] [-t table] ] | [ -f ]

tickerfile is an ascii text file with one ticker per line.
Data for each ticker in tickerfile will pulled and stored
in table "table" in db "db".

table defaults to yahoo_tickers.
db defaults to mobil_db

If -f is specified, the results are written to .csv files.
"""
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2012 gazzman GNU GPL 3."
__contributors__ = []

from datetime import datetime, timedelta
from StringIO import StringIO
from time import sleep
import optparse
import re
import sys
import urllib2

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Date, Text, Float
from sqlalchemy.exc import NoSuchTableError

def pull_from_yahoo(tickers, db='mobil_db', tablename='yahoo_tickers', 
                    totext=False):
    """pull_from_yahoo:

    This script pulls historical ticker data from yahoo and stores 
    it in a postgresql database.
    
    Requires a list of tickers to pull data from, the name of the db,
    and the table where the data should be stored.

    """
    # URL prep
    base_url = 'http://ichart.finance.yahoo.com/table.csv?s='
    xtra = '&g=d&ignore=.csv'

    ua_string = 'Mozilla/5.0 (X11; Linux x86_64; rv:10.0.1)'
    ua_string += ' Gecko/20120225 Firefox/10.0.1'
    header = {'User-Agent' : ua_string}

    # Date prep
    from_date_format = '&a=%(fmonth)s&b=%(fday)s&c=%(fyear)s'
    to_date_format = '&d=%(tmonth)s&e=%(tday)s&f=%(tyear)s'

    ddic = {}
    fdate = datetime.now() - timedelta(days=2)
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
        engine = create_engine('postgresql:///' + db)
        meta = MetaData()
        meta.bind = engine
        conn = engine.raw_connection()
        cur = conn.cursor()

        try:
            prices = Table(tablename, meta, autoload=True)
        except NoSuchTableError as err:
            print >> sys.stderr, tablename, 'doesn\'t exist. Creating...'
            prices = Table(tablename, meta,
                Column('ticker', Text, primary_key=True),
                Column('date', Date, primary_key=True),
                Column('open', Float(24)),
                Column('high', Float(24)),
                Column('low', Float(24)),
                Column('close', Float(24)),
                Column('volume', Float(24)),
                Column('adj_close', Float(24))
            )
            prices.create()

    for ticker in tickers:
        urlticker = ticker.strip().upper()
        url = base_url + urlticker + from_date + to_date + xtra
        ticker = urlticker.replace('%5E','')
        headers = ['ticker']
        mempage = StringIO()

        print >> sys.stderr, 'Starting ' + ticker + '...',
        req = urllib2.Request(url, headers=header)
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

        headers += page.readline().strip().lower().replace(' ','_').split(',')
        for line in page:
            mempage.write(','.join([ticker, line]))
        mempage.seek(0)

        if totext:
            headers = ','.join(headers)
            with open(ticker + '.csv', 'w') as f:
                f.write(headers + '\n')
                f.write(mempage.read() + '\n')
        else:
            # Delete the old ticker data
            prices.delete.where(prices.c.ticker == ticker).execute()

            # Use psycopg2's copy_from to put the csv data into the tale
            cur.copy_from(mempage, tablename, sep=',', columns=headers)
            conn.commit()
        print >> sys.stderr, 'Done!'
        
if __name__ == "__main__":
    p = optparse.OptionParser('%prog filename', version='%prog ' + __version__)
    p.add_option('-d', dest='database', action='store_true', 
        default='mobil_db', help='the name of the database')
    p.add_option('-t', dest='table', action='store_true', 
        default='yahoo_tickers', help='the name of the table')
    p.add_option('-f', dest='totext', action='store_true', 
        default=False, help='the name of the table')
    (options, args) = p.parse_args()

    with open(args[0], 'r') as f:
        tickers = f.read().strip().split('\n')

    pull_from_yahoo(tickers, db=options.database, tablename=options.table, 
                    totext=options.totext)
