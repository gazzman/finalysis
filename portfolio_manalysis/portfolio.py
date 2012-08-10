#!/usr/bin/env python
"""compute_portfolio_retuns 
THIS SCRIPT IS SUSCEPTIBLE TO SQL INJECTION. BE CAREFUL!!!

Compute a time-series of portfolio returns given price data

This script generates a series of portfolio returns given a set of 
weights. It computes both constant and non-constant rebalanced 
portfolio returns.

It requires a comma-delimited file of ticker symbols and initial
weights, from date, and to date as the required first arg and
the name of the table to write the data as the required second arg.
The file should be formatted as:
        ticker, weight, from date, to date
with a date format of %Y-%m-%d

Right now, it only works with a postgresql database as it uses the
psycopg2 module. It requires that the database contain tables or
views of price data, and that those tables or views have the
following fields:
                    date      | date
                    adj_close | real

"""
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2012 gazzman GNU GPL 3."
__contributors__ = []

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

import psycopg2

# db connections
CONN = None
CUR = None

TickerWeight = namedtuple('TickerWeight', ['ticker', 'weight'])
FromTo = namedtuple('FromTo', ['f_date', 't_date'])
########################################################################
# BEGIN function definitions
########################################################################
def _process_portfolio(fname):
    # Create a dictionary of intervals defined by portfolios
    portfolio = OrderedDict()
    rows = csv.reader(open(fname, 'r'))
    for row in rows:
        if not re.match('#', ','.join(row)) and len(row) > 0:
            key = TickerWeight(row[2], row[3])
            value = FromTo(row[0], row[1])
            portfolio[key] = value
    portfolio = OrderedDict(sorted(portfolio.items(), 
                            key=lambda d: d[1].f_date))
    return portfolio    

def _find_intervals(portfolio):
    # Create a list of intervals from a dictionary 
    # of intervals defined by portfolios
    intervals = []
    for key in portfolio:
        f_date = portfolio[key].f_date
        t_date = portfolio[key].t_date
        ilen = len(intervals)
        if ilen == 0:
            intervals.append([f_date, t_date])
        elif intervals[ilen-1][0] != f_date:
            intervals[ilen-1][1] = f_date
            intervals.append([f_date, t_date])
    return intervals

def _portfolio_by_intervals(portfolio, intervals):
    # Create a dictionary of portfolios defined by intervals
    portfolio_intervals = OrderedDict()
    for interval, pkey in product(intervals, portfolio):
        (istart, iend) = interval
        (f_date, t_date) = portfolio[pkey]
        if (f_date <= istart and t_date >= iend):
            ikey = FromTo(istart, iend)
            value = pkey
            if ikey in portfolio_intervals:
                portfolio_intervals[ikey].append(pkey)
            else:
                portfolio_intervals[ikey] = [pkey]
    return portfolio_intervals

def _create_table(returns_table):
    # Create the table of portfolio returns/dollar paths
    try:
        SQL = ('TRUNCATE TABLE ' + returns_table + ';')
        CUR.execute(SQL, ('', ))
        CONN.commit()
    except psycopg2.ProgrammingError as err:
        if re.match('relation .* does not exist', str(err)):
            print  >> sys.stderr, str(err).strip() + '. Creating table...'
            CONN.rollback()
            SQL = ('CREATE TABLE ' + returns_table
                   + ' (date date,'
                   + ' cons_rebal_return double precision,'
                   + ' cons_rebal_dollar double precision,'
                   + ' pass_rebal_return double precision,'
                   + ' pass_rebal_dollar double precision,'
                   + ' PRIMARY KEY (date));')
            CUR.execute(SQL, ('', ))
            CONN.commit()
        else:
            raise err
            
def _create_interval_tables(returns_table, headers):
    # Create a table of portfolio asset returns/dollar paths
    col_names = ' double precision, '.join(headers[1:]) + ' double precision,'
    try:
        SQL = ('TRUNCATE TABLE ' + returns_table + ';')
        CUR.execute(SQL, ('', ))
        CONN.commit()
    except psycopg2.ProgrammingError as err:
        if re.match('relation .* does not exist', str(err)):
            print >> sys.stderr, str(err).strip() + '. Creating table...'
            CONN.rollback()
            SQL = ('CREATE TABLE ' + returns_table 
                   + ' (date date, ' + col_names + ' PRIMARY KEY (date));')
            CUR.execute(SQL, ('', ))
            CONN.commit()
        else:
            raise err

def _create_asset_price_view(prices_view, returns_table, tws, f_date, t_date):
    # Create a view for this interval's portfolio asset prices
    data = {'f_date' : f_date, 't_date' : t_date}
    tickers = [tws[0].ticker]
    datecol = tickers[0] + '.date'
    price_cols = [tickers[0] + '.adj_close ' + tickers[0] + '_adj_close']
    dates = [datecol]
    for tw in tws[1:]:
        tickers.append(tw.ticker)
        price_cols.append(tw.ticker + '.adj_close ' + tw.ticker + '_adj_close')
        dates.append(tw.ticker + '.date')
    dates.reverse()
    dateconds = []
    while len(dates) > 1:
        d0 = dates.pop()
        d1 = dates[len(dates) - 1]
        dateconds.append(d0 + '=' + d1) 
    dateconds = ' AND '.join(dateconds)
    views = ', '.join(tickers)
    col_names = ', '.join(price_cols)
    try:
        SQL = ('DROP VIEW ' + prices_view + ';')
        CUR.execute(SQL, ('', ))
        CONN.commit()
    except psycopg2.ProgrammingError as err:
        if re.match('view .* does not exist', str(err)):
            print >> sys.stderr, str(err).strip(), 'Creating view...'
            CONN.rollback()
        else:
            raise err
    if len(tickers) > 1:            
        SQL_SEL = ('SELECT ' + datecol + ', ' + col_names + ' FROM ' + views
                    + ' WHERE ' + dateconds + ' AND ' 
                    + datecol + ' >= %(f_date)s AND ' 
                    + datecol + ' <= %(t_date)s'
                    + ' ORDER BY date DESC;')
    else:
        SQL_SEL = ('SELECT date, adj_close ' + tickers[0] + '_adj_close'
                   + ' FROM ' + tickers[0] 
                   + ' WHERE date >= %(f_date)s AND date <= %(t_date)s'
                   + ' ORDER BY date DESC;')
    SQL = ('CREATE VIEW ' + prices_view + ' AS ' + SQL_SEL)
    CUR.execute(SQL, data)
    CONN.commit()

    return tickers

def _compute_returns(thisprice, lastprice, lastdollar):
    # Compute returns/dollars given this and last prices and last dollar
    returns = []
    dollars = []
    for p in zip(thisprice, lastprice, lastdollar):
        r = p[1]/p[0]
        returns.append(r)
        dollars.append(p[2] * r)
    return (returns, dollars)

def _compute_ticker_returns(prices_view, returns_table, portfolio_tname, 
                            tickers, intercount):
    # Ascertain column header names
    header_ret = []
    header_dollar = []
    sel_cols = ['date']
    for ticker in tickers:
        sel_cols.append(ticker + '_adj_close')
        header_ret.append(ticker + '_return')
        header_dollar.append(ticker + '_dollar')
    sel_cols = ', '.join(sel_cols)
    headers = ['date'] + header_ret + header_dollar

    # Create the returns/dollar table
    _create_interval_tables(returns_table, headers)

    # Use the last passive dollar value as starting dollar value
    if intercount == 0:
        lastdollar =  [1.0]
    else:
        SQL = ('SELECT pass_rebal_dollar FROM ' + portfolio_tname 
               + ' ORDER BY date DESC LIMIT 1;')
        CUR.execute(SQL, ('', ))
        lastdollar = [CUR.fetchone()[0]]

    # Fetch the prices from the interval's prices view
    SQL = ('SELECT ' + sel_cols + ' FROM ' + prices_view 
           + ' ORDER BY date DESC;')
    CUR.execute(SQL, ('', ))
    prices = CUR.fetchall()

    # From the prices, compute the returns and dollar path
    p = prices.pop()
    (date, lastprice) = (p[0], p[1:])
    lastdollar = len(lastprice) * lastdollar
    mempage = StringIO()
    table_data = csv.writer(mempage)
    while len(prices) > 0:
        p = prices.pop()
        (date, thisprice) = (p[0], p[1:])
        (returns, thisdollar) = _compute_returns(lastprice, thisprice, 
                                                 lastdollar)
        table_data.writerow([date] + returns + thisdollar)
        lastprice = thisprice
        lastdollar = thisdollar

    # Populate the returns/dollar path
    mempage.seek(0)
    CUR.copy_from(mempage, returns_table, sep=',', columns=headers)
    CONN.commit()

def _compute_cret_pdol(prices_view, returns_table, portfolio_view, 
                       portfolio_tname, tws):
    # Create view that is cons ret and pass dollar using weights and returns
    ret_cols = []
    dol_cols = []
    for tw in tws:
        ret_cols.append(tw.ticker + '_return*' + tw.weight)
        dol_cols.append(tw.ticker + '_dollar*' + tw.weight)
    ret_cols = ' + '.join(ret_cols)
    dol_cols = ' + '.join(dol_cols)
    try:
        SQL_SEL = ('SELECT date, ' + ret_cols + ' cons_rebal_return, '
                   + dol_cols + ' pass_rebal_dollar FROM ' 
                   + returns_table + ';')
        SQL = ('CREATE VIEW ' + portfolio_view + ' AS ' + SQL_SEL)
        CUR.execute(SQL, ('', ))
        CONN.commit()
    except psycopg2.ProgrammingError as err:
        if re.match('relation .* already exists', str(err)):
            print >> sys.stderr, str(err).strip(), 'Skipping...'
            CONN.rollback()
        else:
            raise err

    # Insert the view into the portfolio returns/dollars table
    colnames = 'date, cons_rebal_return, pass_rebal_dollar'
    SQL = ('INSERT INTO ' + portfolio_tname + ' (' + colnames + ') SELECT ' 
           + colnames + ' FROM  ' + portfolio_view + ';')
    CUR.execute(SQL, ('', ))
    CONN.commit()

def _compute_cdol_pret(portfolio_view, interval, portfolio_tname, intercount):
    # Fetch the portfolio interval's cons_return and pass_dollar
    SQL = ('SELECT date, cons_rebal_return, pass_rebal_dollar'
           + ' FROM ' + portfolio_view + ' ORDER BY date DESC;')
    CUR.execute(SQL, ('', ))
    cret_pdol = CUR.fetchall()

    # Iterate over the dates from beginning to end and compute
    # cons dollar and pass return
    if intercount == 0:
        (date, lastcdol, lastpdol) = cret_pdol.pop()
        SQL = ('UPDATE ' + portfolio_tname 
               + ' SET (cons_rebal_dollar, pass_rebal_return) ='
               + ' (%(thiscdol)s, %(thispret)s) WHERE date = %(date)s;')
        data = {'date' : date, 'thiscdol' : lastcdol, 'thispret' : lastpdol}
        CUR.execute(SQL, data)
    else:
        SQL = ('SELECT cons_rebal_dollar, pass_rebal_dollar FROM ' + portfolio_tname
               + ' WHERE date = %(date)s;')
        CUR.execute(SQL, {'date' : interval.f_date})
        (lastcdol, lastpdol) = CUR.fetchone()

    while len(cret_pdol) > 0:
        (date, thiscret, thispdol) = cret_pdol.pop()
        thiscdol = lastcdol * thiscret
        thispret = thispdol / lastpdol
        SQL = ('UPDATE ' + portfolio_tname 
               + ' SET (cons_rebal_dollar, pass_rebal_return) ='
               + ' (%(thiscdol)s, %(thispret)s) WHERE date = %(date)s;')
        data = {'date' : date, 'thiscdol' : thiscdol, 'thispret' : thispret}
        CUR.execute(SQL, data)
        lastcdol = thiscdol
        lastpdol = thispdol
    CONN.commit()

def _compute_weights_view(weights_view, returns_table, tickers, tws):
    n = str(len(tws))
    colsum = '(' + '_dollar + '.join(tickers) + '_dollar)*' + n
    weights = []
    for tw in tws:
        weights.append(tw.weight + '*' + tw.ticker + '_dollar/'
                       + colsum + ' ' + tw.ticker + '_weight')
    weights = ', '.join(weights)

    try:
        SQL = ('DROP VIEW ' + weights_view + ';')
        CUR.execute(SQL, ('', ))
        CONN.commit()
    except psycopg2.ProgrammingError as err:
        if re.match('view .* does not exist', str(err)):
            print >> sys.stderr, str(err).strip(), 'Creating view...'
            CONN.rollback()
        else:
            raise err
    SQL_SEL = ('SELECT date, ' + weights + ' FROM ' + returns_table + ';')
    SQL = ('CREATE VIEW ' + weights_view + ' AS ' + SQL_SEL)
    CUR.execute(SQL, ('', ))
    CONN.commit()

def compute_returns(weight_fname, portfolio_tname):
    """evaluate_portfolio_returns:

    Given a weight file path weight_fname as formatted by the 
    gen_weight_file module, use a database of ticker price data 
    to compute a series of returns and store the results in the 
    portfolio_tname table.
    
    """
    # Evaluate portfolio data
    portfolio = _process_portfolio(weight_fname)
    intervals = _find_intervals(portfolio)
    interport = _portfolio_by_intervals(portfolio, intervals)

    # Prep the table
    _create_table(portfolio_tname)

    intercount = 0
    for interval in interport:
        print 'Evaluating interval', intercount
        prices_view = portfolio_tname + '_prices' + str(intercount)
        returns_table = portfolio_tname + '_returns' + str(intercount)
        portfolio_view = portfolio_tname + str(intercount)
        weights_view = portfolio_tname + '_weights' + str(intercount)
        tws = interport[interval]
        
        print '\tCreate price view...'
        tickers = _create_asset_price_view(prices_view, returns_table, tws, 
                                           interval.f_date, interval.t_date)
        print '\t\tCompute returns and dollar paths...'
        _compute_ticker_returns(prices_view, returns_table, portfolio_tname, 
                                tickers, intercount)
        _compute_cret_pdol(prices_view, returns_table, portfolio_view, 
                           portfolio_tname, tws)
        _compute_cdol_pret(portfolio_view, interval, portfolio_tname, 
                           intercount)
        print '\t\t\tCompute weights...'
        _compute_weights_view(weights_view, returns_table, tickers, tws)
        print 'Interval', intercount, 'complete!'
        intercount += 1
########################################################################
# END function definitions
########################################################################
        
if __name__ == "__main__":
    p = optparse.OptionParser('%prog weightfile portfolio_table', 
                              version='%prog ' + __version__)
    p.add_option('-d', dest='database', action='store_true', 
        default='mobil_db', help='the name of the database')
    (options, args) = p.parse_args()

    CONN = psycopg2.connect(database=options.database)
    CUR = CONN.cursor()
    compute_returns(args[0], args[1])
