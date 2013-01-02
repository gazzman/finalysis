#!/usr/bin/python
from calendar import Calendar
from datetime import datetime, timedelta
import csv
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
    ticker = Column(VARCHAR(6), ForeignKey('tickers.ticker'), primary_key=True)
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
    ticker = Column(VARCHAR(6), ForeignKey('tickers.ticker'))
    expiry = Column(DATE)
    call_put = Column(CHAR(1))
    strike = Column(NUMERIC(8,3))
    
    option_prices = relationship('OptionPrice', backref='option_contracts')


class OptionPrice(Base):
    __tablename__ = 'option_prices'
    id = Column(INTEGER, ForeignKey('option_contracts.id'), primary_key=True)
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

        self.s = line + f.read()
        
        self.ticker, self.description, date = data
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


# For running from command line
if __name__ == "__main__":
    file_to_parse = sys.argv[1]
    db_name = sys.argv[2]

    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    cp = ChainParser(file_to_parse)
    data_date = datetime.strptime(cp.date, '%Y-%m-%d').date()
    date_time_dic = {'date': cp.date, 'time': cp.time}

    if not session.query(Ticker).get(cp.ticker):
        session.add(Ticker(ticker=cp.ticker))
        session.commit()

    soup = BeautifulSoup(cp.s)
    underlying_body, option_body = soup.findAll('tbody')

    if not session.query(UnderlyingPrice).get((cp.ticker, cp.date, cp.time)):
        stock = dict([('ticker', cp.ticker)] + date_time_dic.items())
        headers = [th.text for th in underlying_body.findAll('th')]
        data = [td.text for td in underlying_body.findAll('td')]
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

    headers = [th.text for th in option_body.findAll('th')]
    headers = [h.lower() for h in headers]
    c_head, p_head = headers[1:7], headers[9:-1]

    data = [''.join(td.text.split(',')) for td in option_body.findAll('td')]
    data = [d.strip('*') for d in data]

    last_date = None
    last_expiry = None
    while len(data) >= len(headers):
        d, data = data[0:len(headers)], data[len(headers):]
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

        c_price = get_cid(c_con, c_price, session)
        p_price = get_cid(p_con, p_price, session)

        if not session.query(OptionPrice).get((c_price['id'], c_price['date'], 
                                               c_price['time'])):
            session.add(OptionPrice(**c_price))
        if not session.query(OptionPrice).get((p_price['id'], p_price['date'], 
                                               p_price['time'])):
            session.add(OptionPrice(**p_price))

    session.commit()
