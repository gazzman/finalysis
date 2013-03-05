#!/usr/bin/python
__version__ = ".02"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
from StringIO import StringIO
from datetime import datetime
from time import sleep
import argparse
import csv
import re
import sys
import urllib2

try:
    from sqlalchemy import create_engine, MetaData, Table
    from sqlalchemy import Column, Date
    from sqlalchemy.schema import CreateSchema
    from sqlalchemy.exc import ProgrammingError
    from sqlalchemy.dialects.postgresql import NUMERIC, VARCHAR
    have_alchemy = True
except ImportError:
    have_alchemy = False

UA = 'Mozilla/5.0(X11; Linux x86_64; rv:10.0.12)Gecko/20130109 Firefox/10.0.12'
HEADER = {'User-Agent' : UA}
MAX_RETRY = 7

class YahooData:
    dfmt = '%Y%m%d'
    url = 'http://ichart.finance.yahoo.com/table.csv?s=%(ticker)s'
    url += '&a=%(fr_month)i&b=%(fr_day)i&c=%(fr_year)i'
    url += '&d=%(to_month)i&e=%(to_day)i&f=%(to_year)i&g=%(info)s&ignore=.csv'

    def grab_data(self, tickerlist, from_date=None, frequency='d'):
        to = datetime.now()
        f = StringIO()
        if not from_date: fr = datetime.strptime('19000101', self.dfmt)
        else: fr = datetime.strptime(from_date, self.dfmt)
        dates = {'to_year': to.year, 'to_month': to.month-1, 'to_day': to.day,
                 'fr_year': fr.year, 'fr_month': fr.month-1, 'fr_day': fr.day}
        csvout = csv.DictWriter(f, [])
        firstrun = True
        for ticker in tickerlist:
            print >> sys.stderr, 'Starting %s...' % ticker, 
            priceinfo = {'ticker': ticker, 'info': frequency}
            priceinfo.update(dates)
            priceurl = self.url % priceinfo
            pricepage = self.pull_page(priceurl)
            if pricepage:
                prices = self.send_to_csv(pricepage)

                divdic = {}
                divinfo = {'ticker': ticker, 'info': 'v'}
                divinfo.update(dates)
                divurl = self.url % divinfo
                divpage = self.pull_page(divurl)
                dividends = self.send_to_csv(divpage)
                for dividend in dividends:
                    divdic[dividend['date']] = dividend['dividends']

                if firstrun:
                    headers = (['ticker', 'dividends'] + prices.fieldnames)
                    f.write(','.join(headers) + '\n')
                    csvout.fieldnames = headers
                    firstrun = False
                for price in prices:
                    price['ticker'] = ticker
                    if price['date'] in divdic:
                        price['dividends'] = divdic[price['date']]
                    csvout.writerow(price)
                print >> sys.stderr, 'Done!'
            else:
                print >> sys.stderr, '\nProblem with %s. Skipping.' % ticker
        f.seek(0)
        return f

    def send_to_csv(self, page):
        s = page.read()
        headers, delim, data = s.partition('\n')
        headers = [x.lower().strip() for x in headers.split(',')]
        return csv.DictReader(StringIO(data), fieldnames=headers)

    def pull_page(self, url):
        req = urllib2.Request(url, headers=HEADER)
        count = 1
        while count < MAX_RETRY:
            if 4 < count:
                print ' '.join(["\nOk, we're on try", str(count),
                                "now.\nWhy don't you see if this url,",
                                url, "is even working?"])
            try:
                page = urllib2.urlopen(req)
                return page
            except urllib2.HTTPError, err:
                if re.match('HTTP Error 404', str(err)):
                    print >> sys.stderr, '...404 problem...waiting 5 sec...',
                    sleep(5)
                    count += 1
                else:
                    raise err

# For running from command line
if __name__ == "__main__":
    yd = YahooData()
    description = 'Pull ticker price and dividend data from Yahoo! Finance.'
    from_help = 'specify date to pull data from. Format is %s'\
                                                   % yd.dfmt.replace('%', '%%')
    db_help = 'the name of a postgresql database.'
    schema_help = 'an optional database schema.'

    p = argparse.ArgumentParser(description=description)
    p.add_argument('tickerfile', type=str, 
                   help='A text file of ticker sybols to pull, one per line.')
    p.add_argument('-v', '--version', action='version', 
                   version='%(prog)s ' + __version__)
    p.add_argument('--fromdate', help=from_help)
    freq_group = p.add_mutually_exclusive_group()
    freq_group.add_argument('--weekly', action='store_true', 
                            help='Pull weekly data')
    freq_group.add_argument('--monthly', action='store_true',
                            help='Pull monthly data')
    db_group = p.add_argument_group()
    db_group.add_argument('--database', help=db_help)
    db_group.add_argument('--schema', help=schema_help)

    args = p.parse_args()

    with open(args.tickerfile, 'r') as f:
        tickers = [x.strip() for x in f.read().split('\n') if x.strip() != '']
    tickers = [x.upper() for x in tickers if x[0] != '#']    

    if args.weekly:
        freq = 'w'
        tablename = 'yahoo_weekly_prices'
    elif args.monthly:
        freq = 'm'
        tablename = 'yahoo_monthly_prices'
    else:
        freq = 'd'
        tablename = 'yahoo_daily_prices'

    if args.database and not have_alchemy:
        raise EnvironmentError("Python is unable to import sqlalchemy")

    d = yd.grab_data(tickers, frequency=freq, from_date=args.fromdate)
    if args.database:
        dburl = 'postgresql+psycopg2:///' + args.database
        engine = create_engine(dburl)
        try: engine.execute(CreateSchema(args.schema))
        except ProgrammingError: pass
        metadata = MetaData(engine)
        table = Table(tablename, metadata,
                      Column('ticker', VARCHAR(21), index=True, 
                             primary_key=True),
                      Column('date', Date, index=True, primary_key=True),
                      Column('dividends', NUMERIC(19,4)),
                      Column('open', NUMERIC(19,4)),
                      Column('high', NUMERIC(19,4)),
                      Column('low', NUMERIC(19,4)),
                      Column('close', NUMERIC(19,4)),
                      Column('volume', NUMERIC(19,4)),
                      Column('adj_close', NUMERIC(19,4)),
                      schema=args.schema)
        metadata.create_all()
        headers = d.readline().strip().split(',')
        headers = [x.replace(' ', '_') for x in headers]

        # Delete old price data
        delete = table.delete()
        conn = engine.connect()
        for ticker in tickers:
            conn.execute(delete, ticker=ticker)

        # Write new price data
        if args.schema: tablename = '%s.%s' % (args.schema, tablename)
        conn = metadata.bind.raw_connection()
        cur = conn.cursor()
        cur.copy_from(d, tablename, sep=',', null='', columns=headers)
        conn.commit()
    else:
        to = datetime.now()
        fname = 'yprices_%s.csv' % to.strftime('%Y%m%dT%H%M%S')
        with open(fname, 'w') as outfile: outfile.write(d.read())
