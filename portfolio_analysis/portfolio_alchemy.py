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
from sqlalchemy import Date, Float, Text, Integer

from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import and_, select

from sqlalchemy.ext.declarative import declarative_base, declared_attr

PRICETABLENAME = 'yahoo_tickers'
RETURNTABLENAME = 'returns0'
DB = 'mobil_db'
PCOL = 'adj_close'
DCOL = 'date'
TCOL = 'ticker'

engine = create_engine('postgresql+psycopg2:///' + DB, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()
Base.metadata.reflect(engine)
Base.metadata.bind = engine
conn = Base.metadata.bind.connect()

class Returns(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
        
    date = Column(Float(53), primary_key=True)
    passive_return = Column(Float(53))
    passive_dollar = Column(Float(53))
    constant_return = Column(Float(53))
    constant_dollar = Column(Float(53))

        
class PortfolioAssets(Base):
    __table__ = Table(PRICETABLENAME, Base.metadata)

    dates = None
    tickers = None
    weights = None
    query = None
    prices = None

    def __init__(self, assets):
        self.dates = (assets.dates.f_date, assets.dates.t_date)
        (self.tickers, self.weights) = zip(*assets.ticker_weights)
        self.gen_query()

    def gen_query(self):
        self.query = select()
        for ticker in self.tickers:
            a = self.__table__.select(and_(self.__table__.c[TCOL]==ticker, 
                                           self.__table__.c[DCOL]>=f_date, 
                                           self.__table__.c[DCOL]<=t_date
                                           )).alias(ticker)
            if len(self.query.locate_all_froms()) > 0:
                froms = self.query.locate_all_froms()[-1]
                self.query.append_whereclause(froms.c[DCOL].__eq__(a.c[DCOL]))
            else:
                self.query.append_column(a.c[DCOL].label(DCOL))
                self.query.append_order_by(a.c[DCOL].asc())
                self.query.append_whereclause(a.c[DCOL].__ge__(f_date))
                self.query.append_whereclause(a.c[DCOL].__le__(t_date))
            self.query.append_from(a)
            self.query.append_column(a.c[PCOL].label(ticker))

    def populate_price_array(self):
        self.prices = array(Base.metadata.bind.execute(self.query).fetchall())

    def print_prices(self):
        print ','.join(self.query.c.keys())
        for row in Base.metadata.bind.execute(self.query):
            print ','.join(map(str, row))

#class PortfolioReturns(Base, Returns):
#    __table__ = Table(RETURNTABLENAME, Base.metadata)

#    def __init__(self):
#        pass
                
def iterprod(returns):
    dollarpath = array(returns)
    for i in range(1, len(returns)):
        dollarpath[i:] = dollarpath[i:] * returns[i-1]
    return dollarpath

def main(interval, initial_value, returns_tablename='portfolio_interval0'):
    print >> sys.stderr, 'Initializing portfolio assets...',
    pa = PortfolioAssets(interval)
    pa.populate_price_array()
    print >> sys.stderr, 'Done!'

    print >> sys.stderr, 'Fetching data...',
    parray = pa.prices
    print >> sys.stderr, 'Done!'

    # Create the table that stores this interval's return data
    print >> sys.stderr, 'Creating tables...',
    headers = pa.query.c.keys()
    ret_headers = '_return,'.join(headers[1:]) + '_return'
    dp_headers = '_dollar,'.join(headers[1:]) + '_dollar'
    headers = headers[0:1] + ret_headers.split(',') + dp_headers.split(',')
    headers += ['passive_return', 'passive_dollar', 
                'constant_return', 'constant_dollar']

    retable = Table(returns_tablename, Base.metadata)
    retable.drop(checkfirst=True)
    retable.append_column(Column('date', Date, primary_key=True))
    for header in headers[1:]:
        retable.append_column(Column(header, Float(53)))
    retable.create()
    print >> sys.stderr, 'Done!'

    # Compute this interval's return data
    print >> sys.stderr, 'Computing Return Data...',
    w = transpose(array([pa.weights]))
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

    pa.print_prices()


########################################################################
# BEGIN test
########################################################################
f_date = '2011-01-01'
t_date = '2011-01-10'

tickers = ['GFAFX', 'BJBIX', 'BUFSX', 'DODGX']
weights = [.4, .3, .2, .1]

TickerWeight = namedtuple('TickerWeight', ['ticker', 'weight'])
FromTo = namedtuple('FromTo', ['f_date', 't_date'])
Assets = namedtuple('Interval', ['dates', 'ticker_weights'])

tws = []
for tw in zip(tickers, weights):
    tws.append(TickerWeight(tw[0], tw[1]))
ft = FromTo(f_date, t_date)
interval = Assets(ft, tws)

pa = PortfolioAssets(interval)

initial_value = 1
########################################################################
# END test
########################################################################

if __name__ == "__main__":
    main(interval, initial_value)
