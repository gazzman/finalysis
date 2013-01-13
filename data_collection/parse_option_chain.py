#!/usr/bin/python
from calendar import Calendar
from datetime import datetime
from datetime import timedelta as td
from logging.handlers import TimedRotatingFileHandler
import logging
import sys

from BeautifulSoup import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey, Time
from sqlalchemy.orm import backref, relationship, sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import (BOOLEAN, CHAR, DATE, INTEGER, 
                                            NUMERIC, VARCHAR)

#    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
#                     + '%(levelname)6s -- %(threadName)s: %(message)s')
logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                    + '%(levelname)8s: %(message)s')
logger = logging.getLogger(__name__)
hdlr = TimedRotatingFileHandler('parser_output.log', when='midnight')
fmt = logging.Formatter(fmt=logger_format)
hdlr.setFormatter(fmt)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

Base = declarative_base()

class Ticker(Base):
    __tablename__ = 'tickers'
    ticker = Column(VARCHAR(6), primary_key=True)
    type = Column(VARCHAR(6))
    has_options = Column(BOOLEAN)

    underlying_prices = relationship('UnderlyingPrice', backref='tickers')
    option_contracts = relationship('OptionContract', backref='tickers')

class UnderlyingPrice(Base):
    __tablename__ = 'underlying_prices'
    ticker = Column(VARCHAR(6), ForeignKey('tickers.ticker'),
                    primary_key=True, index=True)
    date = Column(DATE, primary_key=True)
    time = Column(Time(timezone=True), primary_key=True)
    price = Column(NUMERIC(8,3))    
    change = Column(NUMERIC(8,3))
    pct_change = Column(NUMERIC(8,3))
    bid = Column(NUMERIC(8,3))
    tick = Column(CHAR(1))
    ask = Column(NUMERIC(8,3))
    bid_size = Column(INTEGER)
    ask_size = Column(INTEGER)
    open = Column(NUMERIC(8,3))
    volume = Column(INTEGER)

class OptionContract(Base):
    __tablename__ = 'option_contracts'
    id = Column(INTEGER, primary_key=True)
    ticker = Column(VARCHAR(6), ForeignKey('tickers.ticker'), index=True)
    expiry = Column(DATE, index=True)
    call_put = Column(CHAR(1))
    strike = Column(NUMERIC(8,3), index=True)
    
    option_prices = relationship('OptionPrice', backref='option_contracts')

class OptionPrice(Base):
    __tablename__ = 'option_prices'
    id = Column(INTEGER, ForeignKey('option_contracts.id'), 
                primary_key=True, index=True)
    date = Column(DATE, primary_key=True)
    time = Column(Time(timezone=True), primary_key=True)
    last = Column(NUMERIC(8,3))
    netchg = Column(NUMERIC(8,3))
    bid = Column(NUMERIC(8,3))
    ask = Column(NUMERIC(8,3))
    vol = Column(INTEGER)
    openint = Column(INTEGER)

class ChainParser():
    def __init__(self, filename, dbname, dbhost=''):
        self.init_db_connection(dbname, dbhost)
        self.parse_data(filename)
        self.dt_date = datetime.strptime(self.date, '%Y-%m-%d').date()
        up_key = (self.ticker, self.date, self.time)
        if not self.session.query(Ticker).get(self.ticker):
            self.add_ticker_to_db()
        if not self.session.query(UnderlyingPrice).get(up_key):
            self.add_up_to_db()
        self.add_contracts_to_db()
        self.session.close()
        logger.info('Session closed')

    def init_db_connection(self, dbname, dbhost):
        logger.info('Connecting to db %s...' % dbname)
        dburl = 'postgresql+psycopg2://%s/%s' % (dbhost, dbname)
        self.engine = create_engine(dburl)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        logger.info('Connected to db %s' % dbname)

    def parse_data(self, filename):
        logger.info('Parsing datafile %s...' % filename)
        start = datetime.now()

        f = open(filename, 'r')
        data = []
        tag_found = False
        while not tag_found:
            line = f.readline().strip()
            try:
                if line[0] == '<': tag_found = True
                else: data.append(line)
            except IndexError:
                pass
        ticker, description, date = data
        self.ticker = '-'.join(ticker.split('/'))
        self.date, self.time = date.split('T')
        soup = BeautifulSoup(line + f.read())
        underlying, options = soup.findAll('tbody')
        self.underlying_headers = [th.text for th in underlying.findAll('th')]
        self.underlying_data = [td.text for td in underlying.findAll('td')]
        contract_headers = [th.text.lower() for th in options.findAll('th')]
        self.call_head = contract_headers[1:7]
        self.put_head = contract_headers[9:-1]
        self.con_data = [''.join(td.text.split(',')).strip('*')
                              for td in options.findAll('td')]
        self.num_headers = len(contract_headers)
        self.num_contracts = 2*len(self.con_data)/self.num_headers

        end = datetime.now()
        logger.info('Parsed datafile %s. Took %0.3f seconds.' 
                         % (filename, self.seconds_elapsed(start, end)))

    def add_ticker_to_db(self):
        logger.info('Adding ticker %s' % self.ticker)
        self.session.add(Ticker(ticker=self.ticker))
        self.session.commit()

    def add_up_to_db(self):
        logger.info('Adding the price for %s at %s'
                            % (self.ticker, 'T'.join([self.date, self.time])))
        start = datetime.now()

        dic = {'ticker': self.ticker, 'date': self.date, 'time': self.time}
        for (h, d) in zip(self.underlying_headers, self.underlying_data):
            if h == 'BxA Size':
                dic['bid_size'], dic['ask_size'] = \
                    [''.join(x.strip().split(',')) for x in d.split('x')]
            elif h == 'Bid [tick]':
                dic['bid'], dic['tick'] = \
                    [''.join(x.strip(' []').split(',')) for x in d.split('[')]
            elif h == '% Change':
                dic['pct_change'] = d.strip('%')
            else:
                dic[h.lower()] = ''.join(d.split(','))
        self.session.add(UnderlyingPrice(**dic))
        self.session.commit()

        end = datetime.now()
        logger.info('Adding price complete. Took %0.3f seconds'
                         % self.seconds_elapsed(start, end))

    def add_contracts_to_db(self):
        logger.info('Adding prices for %i contracts...' 
                         % self.num_contracts) 
        start = datetime.now()

        last_date = None
        last_expiry = None
        self.count = 0
        while len(self.con_data) >= self.num_headers:
            d = self.con_data[0:self.num_headers]
            self.con_data = self.con_data[self.num_headers:]
            c_data, expiry, strike, p_data = d[1:7], d[7], d[8], d[9:-1]
            if last_expiry != expiry:
                last_expiry = expiry
                expiry_date = self.get_expiry_date(last_date, expiry)
                last_date = expiry_date
            else:
                expiry_date = last_date
            base_contract = {'ticker': self.ticker, 'expiry': expiry_date, 
                             'strike': strike}
            self.add_price_to_db(base_contract, 'C', self.call_head, c_data)
            self.add_price_to_db(base_contract, 'P', self.put_head, p_data)
        self.session.commit()

        end = datetime.now()
        logger.info('Adding contract prices complete. Took %0.3f seconds' 
                         % self.seconds_elapsed(start, end))

    def add_price_to_db(self, base_contract, call_put, header, data):
        contract = dict([('call_put', call_put)] + base_contract.items())
        price = dict(zip(header, data)
                     + [('date',self.date), ('time',self.time)])
        missing = len([x for x in price.values() if x.strip() == ''])
        if missing == 0:
            price = self.get_cid(contract, price)
            logger.debug('Got contract id %i' % price['id'])
            sq = self.session.query(OptionPrice).get((price['id'],
                                                 price['date'], price['time']))
            if not sq:
                self.session.add(OptionPrice(**price))
                logger.debug('Added the price for contract %i'
                                  % price['id'])
        else:
            logger.warn('No data for %s%s%s%08i on %s' % (self.ticker, 
                             contract['expiry'].strftime('%y%m%d'), call_put,
                             float(contract['strike'])*1000, self.date))

    def get_expiry_date(self, last_date, expiry):
        data_dow = self.dt_date.isoweekday()
        if 'week' in expiry.lower():
            if last_date: return last_date + td(days=7)
            else: return self.dt_date + td(days=(6 - data_dow))
        elif 'q' in expiry.lower():
            quarter, year = expiry.split('-')
            month = int(quarter[1])*3
            year = int(self.dt_date.strftime('%C') + year)
            c = Calendar()
            last_day = c.monthdatescalendar(year, month)[-1][-1]
            while not (last_day.month == month and last_day.isoweekday() <= 5):
                last_day = last_day - td(days=1)
            return last_day        
        else:
            e_dt = datetime.strptime(expiry, '%b-%y')
            c = Calendar()
            fridays = [x[4] 
                       for x in c.monthdatescalendar(e_dt.year, e_dt.month)]
            if fridays[0].month == e_dt.month: return fridays[2] + td(days=1)
            else: return fridays[3] + td(days=1)

    def get_cid(self, con, price):
        db_con = self.session.query(OptionContract).filter_by(**con).first() 
        if not db_con:
            db_con = OptionContract(**con)
            self.session.add(db_con)
            self.session.commit()
        price['id'] = db_con.id
        return price

    def seconds_elapsed(self, start, end):
        s = (end - start).seconds
        m = (end - start).microseconds
        return round(s + m/1000000.0,3)


# For running from command line
if __name__ == "__main__":
    file_to_parse = sys.argv[1]
    db_name = sys.argv[2]
    cp = ChainParser(file_to_parse, db_name)
