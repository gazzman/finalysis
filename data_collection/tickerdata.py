#!/usr/bin/python
"""tickerdata: Pull ticker and dividend data from web"""
__version__ = ".03"
__author__ = "gazzman"
__copyright__ = "(C) 2012 gazzman GNU GPL 3."
__contributors__ = []

from datetime import datetime, timedelta
from StringIO import StringIO
from time import sleep
import argparse
import csv
import re
import sys
import urllib2

from psycopg2 import IntegrityError
from pytz import timezone
from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, Float

# Column names
ADJCLOSE = 'adj_close'
DATE = 'date'
DIV = 'dividends'

# For converting strings to datetime objects
DATEFORMAT = '%Y-%m-%d %H:%M:%S %Z'

# To avoid problems with duplicate dates
DAYSBACK = 4

def _pull_page(url, header):
    req = urllib2.Request(url, headers=header)
    opened = False
    count = 0
    while not opened:
        if count > 4:
            print ' '.join(['\nOk, we\'re on try', str(count),
                            'now.\nWhy don\'t you see if this url,',
                            url, 'is even working?'])
        try:
            page = urllib2.urlopen(req)
            opened = True
        except urllib2.HTTPError as err:
            if re.match('HTTP Error 404', str(err)):
                print >> sys.stderr, '...404 problem...waiting 5 sec...',
                sleep(5)
                count += 1
            else:
                raise err
    return page

def _add_timezone(date, time='16:00:00', locale='US/Eastern', 
                  fmt='%Y-%m-%d %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return ' '.join([date, time, tzone])

def pull_from_yahoo(tickers, db='mobil_db', schema='yahoo_tickers', 
                    totext=False, update_adj_close=False):
    """pull_from_yahoo(tickers):

    This script pulls historical ticker data and dividend info from 
    yahoo and stores it in a postgresql database or as text.

    Requires a list of tickers to pull data from. Optional arguments
    are:

        db               -- name of db to connect to
        schema           -- schema name to hold tables
        totext           -- if True, output results to text files
        update_adj_close -- if True, update adj_close prices

    We assume the first column is always the DATE column.

    """
    # URL prep
    base_url = 'http://ichart.finance.yahoo.com/table.csv?s='
    prices = '&g=d&ignore=.csv'
    dividends = '&g=v&ignore=.csv'

    ua_string = 'Mozilla/5.0 (X11; Linux x86_64; rv:10.0.1)'
    ua_string += ' Gecko/20120225 Firefox/10.0.1'
    reqheader = {'User-Agent' : ua_string}

    # Date prep
    from_date_format = '&a=%(fmonth)s&b=%(fday)s&c=%(fyear)s'
    to_date_format = '&d=%(tmonth)s&e=%(tday)s&f=%(tyear)s'

    ddic = {}
    tdate = datetime.now() - timedelta(days=DAYSBACK)
    ddic['tmonth'] = str(tdate.month - 1)
    ddic['tday'] = str(tdate.day)
    ddic['tyear'] = str(tdate.year)
    ddic['fmonth'] = str(0)
    ddic['fday'] = str(1)
    ddic['fyear'] = str(1900)

    from_date = from_date_format % ddic
    to_date = to_date_format  % ddic

    if not totext:
        # SQL prep
        engine = create_engine('postgresql+psycopg2:///' + db)
        metadata = MetaData(bind=engine, reflect=True)

        # We use psycopg2 connection for faster updating and inserting
        conn = engine.raw_connection()
        cur = conn.cursor()

    for ticker in tickers:
        # Get the data
        urlticker = ticker.strip().upper()
        url = ''.join([base_url, urlticker, from_date, to_date])
        priceurl = ''.join([url, prices])
        divurl = ''.join([url, dividends])
        ticker = urlticker.replace('%5E','')
        print >> sys.stderr, 'Starting ' + ticker + '...',

        pricepage = csv.DictReader(_pull_page(priceurl, reqheader))
        divpage = csv.DictReader(_pull_page(divurl, reqheader))
        pricepage.fieldnames = map(lambda x: x.lower().replace(' ', '_'), 
                                   pricepage.fieldnames)
        divpage.fieldnames = map(lambda x: x.lower().replace(' ', '_'), 
                                  divpage.fieldnames)

        # Create dividend dictionary
        divdict = {}
        for row in divpage:
            row[DATE] = _add_timezone(row[DATE])
            divdict[row[DATE]] = row[DIV]

        # Add dividend info to csv in memory
        headers = pricepage.fieldnames + [DIV]
        mempage = StringIO()
        memcsv = csv.DictWriter(mempage, headers)
        for row in pricepage:
            row[DATE] = _add_timezone(row[DATE])
            if row[DATE] in divdict:
                row[DIV] = divdict[row[DATE]]
            else:
                row[DIV] = None
            memcsv.writerow(row)

        mempage.seek(0)
        if totext:
            # Send to file
            with open(''.join([ticker, '.csv']), 'w') as f:
                f.write(','.join(headers))
                f.write('\n')
                f.write(mempage.read())
        else:
            # Send to db
            table = Table(ticker.lower(), metadata, schema=schema)
            table.append_column(Column(DATE, DateTime(True), primary_key=True))
            for header in headers[1:]:
                table.append_column(Column(header, Float(24)))
            table.create(checkfirst=True)
            try:
                tablename = '.'.join([schema, ticker.lower()])
                cur.copy_from(mempage, tablename, sep=',', null='', 
                            columns=headers)
                conn.commit()
            except IntegrityError as err:
                conn.rollback()
                mempage.seek(0)
                memcsv = csv.DictReader(mempage, fieldnames=headers)
                m = re.search('\d+-\d+-\d+ \d+:\d+:\d+ E[DS]T', str(err))
                lastdate = datetime.strptime(m.group(0), DATEFORMAT)
                (upnot, innot) = (False, False)
                for row in memcsv:
                    if row[DIV] is '': row.pop(DIV)
                    thisdate = datetime.strptime(row[DATE], DATEFORMAT)
                    if thisdate > lastdate:
                        # Insert new dates
                        if not innot:
                            msg = ('Inserting from ' + row[DATE] + '...')
                            print >> sys.stderr, msg,
                            innot = True
                        ins = table.insert().values(row)
                        cur.execute(str(ins), row)
                    elif update_adj_close:
                        # Update adj_close
                        if not upnot:
                            msg = ('Updating ' + ADJCLOSE + ' backwards from ' 
                                   + row[DATE] + '...')
                            print >> sys.stderr, msg,
                            upnot = True
                        upd = table.update().where(table.c[DATE]==row[DATE])
                        upd = upd.values({ADJCLOSE: row[ADJCLOSE]})
                        cur.execute(str(upd), {ADJCLOSE: row[ADJCLOSE], 
                                               'date_1': row[DATE]})
                conn.commit()
        print >> sys.stderr, 'Done!'

if __name__ == "__main__":
    description = 'Pull ticker data from web.'
    description += ' Can direct output to disk or to database.'
    description += ' If db, must be a postgres db.'
    description += ' You can specify the schema and db name.'

    ddef = 'mobil_db'
    dhelp = 'name of db in which to store the data.'
    dhelp += ' Defaults to \'' + ddef + '\'.'
    sdef = 'yahoo_tickers'
    shelp = 'name of schema in which to hold data tables.'
    shelp += ' Defaults to \'' + sdef + '\'.'

    p = argparse.ArgumentParser(description=description)
    p.add_argument('tickerfile', type=str, 
                   help='A text file of ticker sybols, one per line.')
    p.add_argument('-v', '--version', action='version', 
                   version='%(prog)s ' + __version__)

    g1 = p.add_argument_group()        
    g1 = p.add_argument_group('Store results in postgres database')
    g1.add_argument('-d', metavar='db', default=ddef, dest='db', 
                    help=dhelp)
    g1.add_argument('-s', metavar='schema', default=sdef, dest='schema', 
                    help=shelp)
    g1.add_argument('-u', dest='update', action='store_true', 
                    help='update historical adj_close prices')

    g2 = p.add_argument_group('Write to file(s)')    
    g2.add_argument('-f', dest='totext', action='store_true', 
                    help='write data to \'ticker_symbol\'.csv file(s)')
    args = p.parse_args()

    with open(args.tickerfile, 'r') as f:
        tickers = f.read().strip().split('\n')

    pull_from_yahoo(tickers, db=args.db, schema=args.schema, 
                    totext=args.totext, update_adj_close=args.update)
