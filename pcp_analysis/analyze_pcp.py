#!/usr/bin/python
from calendar import Calendar
from datetime import datetime, timedelta
from decimal import Decimal
from logging.handlers import TimedRotatingFileHandler
from math import exp, log
import logging
import re
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import aliased, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from finalysis.data_collection.parse_option_chain import (UnderlyingPrice,
                                                          OptionContract,
                                                          OptionPrice)

logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

Base = declarative_base()

stock = aliased(UnderlyingPrice, name='stock')
call_contract = aliased(OptionContract, name='contract')
put_contract = aliased(OptionContract)
call = aliased(OptionPrice, name='call')
put = aliased(OptionPrice, name='put')

class PCPAnalyzer():
#    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
#                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s: %(message)s')
    logger = logging.getLogger('PCPAnalyzer')

    def __init__(self, dbname, dbhost=''):
        self.init_logger()
        self.init_db_connection(dbname, dbhost)

    def init_logger(self):
        hdlr = TimedRotatingFileHandler('pcp_analyzer.log', when='midnight')
        fmt = logging.Formatter(fmt=self.logger_format)
        hdlr.setFormatter(fmt)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.INFO)

    def init_db_connection(self, dbname, dbhost):
        self.logger.info('Connecting to db %s...' % dbname)
        dburl = 'postgresql+psycopg2://%s/%s' % (dbhost, dbname)
        self.engine = create_engine(dburl)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.logger.info('Connected to db %s' % dbname)

    def seconds_elapsed(self, start, end):
        s = (end - start).seconds
        m = (end - start).microseconds
        return round(s + m/1000000.0,3)

    def gen_contract_id(self, row, call_put):
        return ('%s%s%s%08i' % (row['stock_ticker'],
                                row['contract_expiry'].strftime('%y%m%d'), 
                                call_put.upper(),
                                row['contract_strike']*1000))

    def solve_for_r(self, numerator, desc, row):
        days_to_expiry = row['contract_expiry'] - row['stock_date']
        try:
            rate = log(numerator)
            rate -= log(row['contract_strike'])
            rate *= -365.0/days_to_expiry.days
        except ZeroDivisionError:
            pass
        except ValueError as err:
            if numerator <= 0:
                rate = None
                errmsg = '%s call exceeds stock plus put for %s at'
                errmsg += ' strike %9.3f with expiry %s on %s at %s'
                errdata = (desc, row['stock_ticker'], row['contract_strike'],
                           row['contract_expiry'].isoformat(),
                           row['stock_date'].isoformat(),
                           row['stock_time'].isoformat())
                self.logger.error(errmsg % errdata)
            else: raise err
        return rate

    def zero_cash_in_rates(self, row):
        ls_num = row['stock_bid'] + row['put_bid'] - row['call_ask']
        sl_num = row['stock_ask'] + row['put_ask'] - row['call_bid']
        return (self.solve_for_r(ls_num, 'Long-short', row), 
                self.solve_for_r(sl_num, 'Short-long', row))

    def cash_in_today(self, row, r_lend, r_borrow):
        days_to_expiry = row['contract_expiry'] - row['stock_date']
        ls_df = Decimal(str(exp(-r_lend*days_to_expiry.days/365.0)))
        sl_df = Decimal(str(exp(-r_borrow*days_to_expiry.days/365.0)))
        ls_cash = row['call_ask'] + row['contract_strike']*ls_df
        ls_cash -= (row['stock_bid'] + row['put_bid'])
        sl_cash = -row['call_bid'] - row['contract_strike']*sl_df
        sl_cash += (row['stock_ask'] + row['put_ask'])
        return ls_cash, sl_cash

    def base_query(self):
        q = self.session.query(
                               stock.date,
                               stock.time, 
                               stock.ticker, 
                               stock.ask, 
                               stock.bid, 
                               call.id, 
                               put.id, 
                               call_contract.expiry,
                               call_contract.strike, 
                               call.ask, 
                               call.bid, 
                               put.ask, 
                               put.bid
                               ).\
                filter(call_contract.call_put=='C').\
                filter(put_contract.call_put=='P').\
                filter(stock.date==call.date).\
                filter(call.date==put.date).\
                filter(stock.time==call.time).\
                filter(call.time==put.time).\
                filter(stock.ticker==call_contract.ticker).\
                filter(call_contract.ticker==put_contract.ticker).\
                filter(call_contract.strike==put_contract.strike).\
                filter(call_contract.expiry==put_contract.expiry).\
                filter(call_contract.id==call.id).\
                filter(put_contract.id==put.id)
        return q


# For running from command line
if __name__ == "__main__":
    db_name = sys.argv[1]
    db_host = sys.argv[2]
    ticker = sys.argv[3]
    date = sys.argv[4]
    p = PCPAnalyzer(db_name, dbhost=db_host)
    r = p.session.execute(p.base_query().filter(stock.date == date).filter(stock.ticker == ticker).filter(call.bid > 0, put.bid > 0).order_by(call_contract.id, stock.date, stock.time).limit(10000)).fetchall()
    for i in r:
        print i['stock_date'], i['stock_time'], p.gen_contract_id(i, 'C'), p.gen_contract_id(i, 'P'), p.cash_in_today(i, .0015, .0015), p.zero_cash_in_rates(i)

    p.session.close()        
