#!/usr/bin/python
from StringIO import StringIO
from collections import namedtuple
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
from datetime import datetime
from itertools import product
import csv
import optparse
import re
import sys
import time
import urllib2

from numpy import (array, apply_along_axis, dot, concatenate, 
                  hstack, transpose, vstack)

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, ForeignKey
from sqlalchemy import Date, Float, Text

from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import and_, select

from sqlalchemy.ext.declarative import declarative_base, declared_attr

PRICETABLENAME = 'yahoo_tickers'
DB = 'mobil_db'
PCOL = 'adj_close'
DCOL = 'date'
TCOL = 'ticker'

engine = create_engine('postgresql+psycopg2:///' + DB, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()
Base.metadata.reflect(engine)

class MyMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
        
class Prices(Base):
    __table__ = Base.metadata.tables[PRICETABLENAME]

    def __init__(self, interval):
        self.interval = interval
        (f_date, t_date) = (interval.dates.f_date, interval.dates.t_date)
        (self.tickers, self.weights) = zip(*interval.ticker_weights)
        self.s = select()
        self.s.bind = self.__table__.bind
        for ticker in self.tickers:
            a = self.__table__.select(and_(self.__table__.c[TCOL]==ticker, 
                                           self.__table__.c[DCOL]>=f_date, 
                                           self.__table__.c[DCOL]<=t_date
                                           )).alias(ticker)
            if len(self.s.locate_all_froms()) > 0:
                lastfrom = self.s.locate_all_froms()[-1]
                self.s.append_whereclause(lastfrom.c[DCOL].__eq__(a.c[DCOL]))
            else:
                self.s.append_column(a.c[DCOL].label(DCOL))
                self.s.append_order_by(a.c[DCOL].asc())
                self.s.append_whereclause(a.c[DCOL].__ge__(f_date))
                self.s.append_whereclause(a.c[DCOL].__le__(t_date))
            self.s.append_from(a)
            self.s.append_column(a.c[PCOL].label(ticker))

    def print_(self, conn):
        print ','.join(self.s.c.keys())
        for row in conn.execute(self.s):
            print ','.join(map(str, row))

#class Returns(Prices):
#    date = Column(Date)
                
def iterprod(returns):
    dollarpath = array(returns)
    for i in range(1, len(returns)):
        dollarpath[i:] = dollarpath[i:] * returns[i-1]
    return dollarpath


def main(interval, initial_value, returns_tablename='portfolio_interval0'):
    print >> sys.stderr, 'Initializing query...',
    prices = Prices(interval)
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Creating engine and binding...',
    engine = create_engine('postgresql+psycopg2:///' + DB, echo=False)
    meta = MetaData()
    meta.bind = engine
    conn = engine.connect()    
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Fetching data...',
    parray = array(conn.execute(prices.s).fetchall())
    print >> sys.stderr, 'Done!'

    # Create the table that stores this interval's return data
    print >> sys.stderr, 'Creating tables...',
    headers = prices.s.c.keys()
    ret_headers = '_return,'.join(headers[1:]) + '_return'
    dp_headers = '_dollar,'.join(headers[1:]) + '_dollar'
    headers = headers[0:1] + ret_headers.split(',') + dp_headers.split(',')
    headers += ['passive_return', 'passive_dollar', 
                'constant_return', 'constant_dollar']
    columns = [Column('date', Date, primary_key=True)]
    for header in headers[1:]:
        columns.append(Column(header, Float(53)))
    retable = Table(returns_tablename, meta, *columns)
    retable.drop(checkfirst=True)
    retable.create(checkfirst=True)
    print >> sys.stderr, 'Done!'

    # Compute this interval's return data
    print >> sys.stderr, 'Computing Return Data...',
    w = transpose(array([prices.weights]))
    dates = parray[1:,0:1]
    returns = parray[1:,1:]/parray[:-1,1:]
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Computing Dollars...',
    dollars = apply_along_axis(iterprod, 0, returns)
    print >> sys.stderr, 'Done!'


    print >> sys.stderr, 'Computing Passive Return and Dollar...',
    passive_dollar = dot(dollars, w)
    passive_return = passive_dollar[1:]/passive_dollar[:-1]
    passive_return = vstack((passive_dollar[[0]], passive_return))
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Computing Constant Return and Dollar...',
    constant_return = dot(returns, w)
    constant_dollar = apply_along_axis(iterprod, 0, constant_return)
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Prepping for insertion...',
    returns = concatenate((dates, returns, dollars, 
                           passive_return, passive_dollar, 
                           constant_return, constant_dollar), 1)
    returns = returns.tolist()
    returns.reverse()
    inslist = []
    while len(returns) > 0:
        row = returns.pop()
        insdic = {}
        for key, value in zip(headers, row):
            insdic[key] = value
        inslist.append(insdic)
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Inserting into table...',
    conn.execute(retable.insert(), inslist)
    print >> sys.stderr, 'Done!'

    prices.print_(conn)

if __name__ == "__main__":
    f_date = '2011-01-01'
    t_date = '2011-01-10'

    tickers = ['GFAFX', 'BJBIX', 'BUFSX', 'DODGX']
    weights = [.4, .3, .2, .1]

    TickerWeight = namedtuple('TickerWeight', ['ticker', 'weight'])
    FromTo = namedtuple('FromTo', ['f_date', 't_date'])
    Interval = namedtuple('Interval', ['dates', 'ticker_weights'])

    tws = []
    for tw in zip(tickers, weights):
        tws.append(TickerWeight(tw[0], tw[1]))
    ft = FromTo(f_date, t_date)
    interval = Interval(ft, tws)

    initial_value = 1

    main(interval, initial_value)
