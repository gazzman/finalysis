#!/usr/bin/python
"""tickerdata: Pull ticker and dividend data from web

This module provides a method that returns an SQLAlchemy ORM class
that maps to a table of ticker price data.

The class includes methods that automatically insert and update the db
data from the web, as well as a method that writes the data to a
csv file if the user wishes to.

This module may also be run from the command line, in which case a 
postgresql db connection must be available.

Note: if a schema is specified, it must already exist in the db.

"""
__version__ = ".07"
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

from psycopg2 import IntegrityError
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import DateTime, Float, Text
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

from helpers._helpers import _DateHelpers, _WebHelpers

Base = declarative_base()

class PriceMixin(object):
    """A Mixin for an SQLAlchemy ORM containing price data"""
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    date = Column(DateTime(True), primary_key=True)
    ticker = Column(Text, primary_key=True)

class YahooMixin(_DateHelpers, _WebHelpers):
    """A Mixin of methods for a SQLAlchemy ORM of yahoo data"""
    # Some default column names we'll use later
    adjclosecol = 'adj_close'
    datecol = 'date'
    divcol = 'dividends'
    daysback = 4
    dateformat = '%Y-%m-%d %H:%M:%S %Z'

    def pull_tickerdata(self, ticker, daysback=4):
        """Pull price and dividend history from yahoo finance.

        ticker is the ticker symbol to pull data for.

        Returns a tuple containing a list of lowercase column header
        names where any spaces have been converted to '_' 
        and a StringIO() object that contains the comma delimited data.

        Keyword argument:
            daysback -- Denotes how many days back from today will be
                        the end-date. Sometimes, yahoo returns the same
                        date for the same ticker twice, leading to 
                        primary key IntegrityErrors.
                        Defaults to 4 days.

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
        tdate = datetime.now() - timedelta(days=daysback)
        ddic['tmonth'] = str(tdate.month - 1)
        ddic['tday'] = str(tdate.day)
        ddic['tyear'] = str(tdate.year)
        ddic['fmonth'] = str(0)
        ddic['fday'] = str(1)
        ddic['fyear'] = str(1900)

        from_date = from_date_format % ddic
        to_date = to_date_format  % ddic

        # Get the data
        urlticker = ticker.strip().upper()
        url = ''.join([base_url, urlticker, from_date, to_date])
        priceurl = ''.join([url, prices])
        divurl = ''.join([url, dividends])
        ticker = urlticker.replace('%5E','')
        print >> sys.stderr, 'Grabbing data for ' + ticker + ' from web...',
        pricepage = csv.DictReader(self._pull_page(priceurl, reqheader))
        divpage = csv.DictReader(self._pull_page(divurl, reqheader))
        pricepage.fieldnames = map(lambda x: x.lower().replace(' ', '_'), 
                                   pricepage.fieldnames)
        divpage.fieldnames = map(lambda x: x.lower().replace(' ', '_'), 
                                 divpage.fieldnames)

        # Create dividend dictionary
        divdict = {}
        for row in divpage:
            row[self.datecol] = self._add_timezone(row[self.datecol])
            divdict[row[self.datecol]] = row[self.divcol]

        # Add ticker and dividend info to csv in memory
        headers = ['ticker'] + pricepage.fieldnames + [self.divcol]
        mempage = StringIO()
        memcsv = csv.DictWriter(mempage, headers)
        for row in pricepage:
            row['ticker'] = ticker
            row[self.datecol] = self._add_timezone(row[self.datecol])
            if row[self.datecol] in divdict:
                row[self.divcol] = divdict[row[self.datecol]]
            else:
                row[self.divcol] = None
            memcsv.writerow(row)
        mempage.seek(0)
        print >> sys.stderr, 'Done!'
        return (headers, mempage)

    def update_prices(self, tickers, update=False, refresh=False):
        """Insert or update price data in table.

        tickers is a list of ticker symbols.

        For each ticker symbol, pull the price and dividend info 
        from yahoo and upload to the table. If data already exists,
        then default behavior is to only insert data for new dates.

        Keyword arguments:        
            refresh -- If True, then delete and overwrite existing data.
            update  -- If True, then update existing adj_close prices.
        
        """
        # We use the fast psycopg2 connection
        conn = self.metadata.bind.raw_connection()
        cur = conn.cursor()
        for ticker in tickers:
            (headers, mempage) = self.pull_tickerdata(ticker, self.daysback)
            ticker = ticker.replace('%5E', '')
            print >> sys.stderr, 'Writing ' + ticker + ' data to db...',
            t_eq = self.__table__.c.ticker.__eq__(ticker)
            if refresh:
                self.__table__.delete(whereclause=t_eq).execute()
            try:
                cur.copy_from(mempage, self.__tablename__, sep=',', null='',
                              columns=headers)
                conn.commit()
            except IntegrityError as err:
                conn.rollback()
                mempage.seek(0)
                m = re.search('\d+-\d+-\d+ \d+:\d+:\d+ E[DS]T', str(err))
                lastdate = datetime.strptime(m.group(0), self.dateformat)
                (upnot, innot) = (False, False)
                memcsv = csv.DictReader(mempage, fieldnames=headers)
                for row in memcsv:
                    if row[self.divcol] is '': row.pop(self.divcol)
                    thisdate = datetime.strptime(row[self.datecol], 
                                                 self.dateformat)
                    if thisdate > lastdate:
                        # Insert new dates
                        if not innot:
                            msg = ('Inserting from ' + row[self.datecol]
                                   + '...')
                            print >> sys.stderr, msg,
                            innot = True
                        ins = self.__table__.insert().values(row)
                        cur.execute(str(ins), row)
                    elif update:
                        # Update adj_close
                        if not upnot:
                            msg = ('Updating ' + self.adjclosecol 
                                   + ' backwards from ' + row[self.datecol]
                                   + '...')
                            print >> sys.stderr, msg,
                            upnot = True
                        d_eq = self.__table__.c[self.datecol]
                        d_eq = d_eq.__eq__(row[self.datecol])
                        td_eq = d_eq.__and__(t_eq)
                        upd = self.__table__.update().where(td_eq)
                        upd = upd.values({self.adjclosecol: 
                                          row[self.adjclosecol]})
                        cur.execute(str(upd), 
                                    {self.adjclosecol: row[self.adjclosecol], 
                                     'date_1': row[self.datecol],
                                     'ticker_1': ticker})
                conn.commit()
            print >> sys.stderr, 'Done!'

    def write_tickerdata_to_file(self, tickers):
        """Write price and dividend data to ./'ticker'.csv file."""
        for ticker in tickers:
            (headers, mempage) = self.pull_tickerdata(ticker)
            ticker = ticker.replace('%5E','')
            print >> sys.stderr, 'Writing ' + ticker + ' data to file...',
            with open(''.join([ticker, '.csv']), 'w') as f:
                f.write(','.join(headers))
                f.write('\n')
                f.write(mempage.read())
            print >> sys.stderr, 'Done!'

def gen_yahoo_prices_table(tablename, schema=None, method_dict={}, 
                           headers=['tickers', 'date', 'open', 'high', 'low', 
                                    'close', 'volume', 'adj_close', 
                                    'dividends']):
    """Create an SQLAlchemy ORM to a table of yahoo price data.

    tablename is the name of the table to map.
            
    This function returns an sqlalcemy declarative_base() object with
    attributes and methods inherited from PriceMixin and YahooMixin.

    Keyword arguments:
        schema      -- The name of the schema Not all dbs support this,
                       and schema must already exist in db.
        method_dict -- Additional data and methods to pass to class.
        headers     -- list of data headers; defaults to current yahoo
                       headers.

    """
    for header in headers:
        if not re.search('date|ticker', header.lower()):
            method_dict[header.lower()] = Column(Float(23))
    TableClass = type(tablename, (PriceMixin, YahooMixin, Base), method_dict)
    if schema is not None:
        TableClass.__table__.schema = schema
    return TableClass

# For running from command line
if __name__ == "__main__":
    description = 'Pull ticker data from web.'
    description += ' Can direct output to disk or to database.'
    description += ' If db, must be a postgres db.'
    description += ' You can specify the schema and db name.'

    ddef = 'mobil_db'
    dhelp = 'name of db in which to store the data.'
    dhelp += ' Defaults to \'' + ddef + '\'.'
    sdef = None
    shelp = 'name of schema in which to hold data table.'
    shelp += ' Defaults to \'' + str(sdef) + '\'.'
    tdef = 'yahoo'
    thelp = 'name of table in which to store data.'
    thelp += ' Defaults to \'' + tdef + '\'.'

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
    g1.add_argument('-t', metavar='tablename', default=tdef, dest='tablename', 
                    help=thelp)
    g1.add_argument('-r', dest='refresh', action='store_true', 
                    help='delete and refresh data')
    g1.add_argument('-u', dest='update', action='store_true', 
                    help='update historical adj_close prices')

    g2 = p.add_argument_group('Write to file(s)')    
    g2.add_argument('-f', dest='totext', action='store_true', 
                    help='write data to \'ticker_symbol\'.csv file(s)')
    args = p.parse_args()

    with open(args.tickerfile, 'r') as f:
        tickers = f.read().strip().split('\n')

    T = gen_yahoo_prices_table(args.tablename, schema=args.schema)
    t = T()
    if args.totext:
        t.write_tickerdata_to_file(tickers)
    else:
        engine = create_engine('postgresql+psycopg2:///' + args.db, echo=False)
        Base.metadata.bind = engine
        Base.metadata.reflect(schema=args.schema)
        t.__table__.create(checkfirst=True)
        t.update_prices(tickers, update=args.update, refresh=args.refresh)
