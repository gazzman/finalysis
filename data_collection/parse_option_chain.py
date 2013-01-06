#!/usr/bin/python
from calendar import Calendar
from datetime import datetime, timedelta
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
    def __init__(self, the_file, port='5432'):
        f = open(the_file, 'r')
        tag_found = False
        data = []
        while not tag_found:
            line = f.readline().strip()
            try:
                if line[0] == '<': tag_found = True
                else: data.append(line)
            except IndexError:
                pass

        soup = BeautifulSoup(line + f.read())
        self.underlying_body, self.option_body = soup.findAll('tbody')
        
        ticker, self.description, date = data
        self.ticker = '-'.join(ticker.split('/'))
        self.date, self.time = date.split('T')


def get_expiry_date(last_date, data_date, expiry):
    data_dow = data_date.isoweekday()
    if 'week' in expiry.lower():
        if last_date: return last_date + timedelta(days=7)
        else: return data_date + timedelta(days=(5 - data_dow))
    elif 'q' in expiry.lower():
        quarter, year = expiry.split('-')
        month = int(quarter[1])*3
        year = int(year)
        c = Calendar()
        last_day = c.monthdatescalendar(year, month)[-1][-1]
        while not (last_day.month == month and last_day.isoweekday() <= 5):
            last_day = last_day - timedelta(days=1)
        return last_day        
    else:
        e_dt = datetime.strptime(expiry, '%b-%y')
        c = Calendar()
        fridays = [x[4] for x in c.monthdatescalendar(e_dt.year, e_dt.month)]
        if fridays[0].month == e_dt.month: return fridays[2]
        else: return fridays[3]        


def get_cid(con_dic, price_dic, session):
    db_con = session.query(OptionContract).filter_by(**con_dic).first() 
    if not db_con:
        db_con = OptionContract(**con_dic)
        session.add(db_con)
        session.commit()
    price_dic['id'] = db_con.id
    return price_dic


def seconds_elapsed(start, end):
    s = (end - start).seconds
    m = (end - start).microseconds
    return round(s + m/1000000.0,3)

# For running from command line
if __name__ == "__main__":
    file_to_parse = sys.argv[1]
    db_name = sys.argv[2]
    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s -- %(threadName)s: %(message)s')

    logger = logging.getLogger('parse_option_chain')
    hdlr = TimedRotatingFileHandler('parser_output.log', when='midnight')
    fmt = logging.Formatter(fmt=logger_format)
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info('Parsing datafile ' + file_to_parse)
    parse_start = datetime.now()
    cp = ChainParser(file_to_parse)
    data_date = datetime.strptime(cp.date, '%Y-%m-%d').date()
    date_time_dic = {'date': cp.date, 'time': cp.time}

    if not session.query(Ticker).get(cp.ticker):
        logger.info('Adding ticker ' + cp.ticker + ' to ' + db_name)
        session.add(Ticker(ticker=cp.ticker))
        session.commit()

    o_headers = [th.text.lower() for th in cp.option_body.findAll('th')]
    c_head, p_head = o_headers[1:7], o_headers[9:-1]

    o_data = [''.join(td.text.split(',')) for td in cp.option_body.findAll('td')]
    o_data = [d.strip('*') for d in o_data]
    parse_end = datetime.now()
    logger.info('Parsing complete. Took ' 
                + str(seconds_elapsed(parse_start, parse_end)) + ' seconds.')

    if not session.query(UnderlyingPrice).get((cp.ticker, cp.date, cp.time)):
        logger.info('Adding the price for ' + cp.ticker + ' at ' 
                    + 'T'.join([cp.date, cp.time]))
        add_price_start = datetime.now()
        stock = dict([('ticker', cp.ticker)] + date_time_dic.items())
        headers = [th.text for th in cp.underlying_body.findAll('th')]
        data = [td.text for td in cp.underlying_body.findAll('td')]
        for (h, d) in zip(headers, data):
            if h == 'BxA Size':
                stock['bid_size'], stock['ask_size'] = [''.join(x.strip().split(','))
                                                        for x in d.split('x')]
            elif h == 'Bid [tick]':
                stock['bid'], stock['tick'] = [''.join(x.strip(' []').split(','))
                                               for x in d.split('[')]
            elif h == '% Change':
                stock['pct_change'] = d.strip('%')
            else:
                stock[h.lower()] = ''.join(d.split(','))

        session.add(UnderlyingPrice(**stock))
        session.commit()
        add_price_end = datetime.now()
        logger.info('Adding price complete. Took ' 
                + str(seconds_elapsed(add_price_start, add_price_end))
                + ' seconds.')
    else: logger.info(cp.ticker + ' price data already in db for ' 
                      + 'T'.join([cp.date, cp.time]))

    logger.info('Adding prices for ' + str(len(o_data)/len(o_headers)) 
                + ' contracts.')
    add_prices_start = datetime.now()
    last_date = None
    last_expiry = None
    while len(o_data) >= len(o_headers):
        d, o_data = o_data[0:len(o_headers)], o_data[len(o_headers):]
        c_data, expiry, strike, p_data = d[1:7], d[7], d[8], d[9:-1]

        if last_expiry != expiry:
            last_expiry = expiry
            expiry_date = get_expiry_date(last_date, data_date, expiry)
            last_date = expiry_date
        else:
            expiry_date = last_date

        contract = {'ticker': cp.ticker, 'expiry': expiry_date, 
                    'strike': strike}

        c_con = dict([('call_put', 'C')] + contract.items())
        p_con = dict([('call_put', 'P')] + contract.items())

        c_price = dict(zip(c_head, c_data) + date_time_dic.items())
        p_price = dict(zip(p_head, p_data) + date_time_dic.items())

        # Determine if the data is missing
        c_nc = len([x for x in c_price.values() if x.strip() == ''])
        p_nc = len([x for x in p_price.values() if x.strip() == ''])

        # Get the contract ID
        c_price = get_cid(c_con, c_price, session)
        logger.debug('Got contract id ' + str(c_price['id']))
        p_price = get_cid(p_con, p_price, session)
        logger.debug('Got contract id ' + str(p_price['id']))

        # See if this price is already in the db
        c_sq =session.query(OptionPrice).get((c_price['id'], c_price['date'], 
                                              c_price['time']))
        p_sq =session.query(OptionPrice).get((p_price['id'], p_price['date'], 
                                              p_price['time']))

        # Add if data isn't missing and the price isn't already in db
        if not (c_sq or c_nc == 0):
            session.add(OptionPrice(**c_price))
            logger.debug('Adding the price for contract ' + str(c_price['id']))

        if not (p_sq or p_nc == 0):
            session.add(OptionPrice(**p_price))
            logger.debug('Adding the price for contract ' + str(p_price['id']))

    session.commit()
    add_prices_end = datetime.now()
    logger.info('Adding contract prices complete. Took ' 
            + str(seconds_elapsed(add_prices_start, add_prices_end))
            + ' seconds.')    
