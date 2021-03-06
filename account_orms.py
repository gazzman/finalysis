#!/usr/bin/python
from datetime import datetime
from decimal import Decimal

from pytz import timezone
from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import CHAR, NUMERIC, VARCHAR

Base = declarative_base()

SCHEMA = 'portfolio'

class Account(Base):
    __tablename__ = 'accounts'
    __table_args__ = {'schema':SCHEMA}
    id = Column(Integer, primary_key=True, unique=True)
    institution = Column(String, primary_key=True)
    account = Column(String, primary_key=True)
    positions = relationship('Position', backref='accounts')

class Position(Base):
    __tablename__ = 'positions'
    __table_args__ = {'schema':SCHEMA}
    id = Column(Integer, 
                ForeignKey('%s.accounts.id' % SCHEMA, onupdate='cascade'),
                primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True, index=True)
    symbol = Column(VARCHAR(21), primary_key=True, index=True)
    description = Column(String, primary_key=True, index=True)
    qty = Column(NUMERIC(17,4))
    price = Column(NUMERIC(16,3))
    total_value = Column(NUMERIC(16,3))

class Transaction(Base):
    __tablename__ = 'transactions'
    __table_args__ = {'schema':SCHEMA}
    id = Column(Integer, 
                ForeignKey('%s.accounts.id' % SCHEMA, onupdate='cascade'),
                primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True, index=True)
    activityDescription = Column(String, primary_key=True, index=True)
    amount = Column(NUMERIC(19,4), primary_key=True)
    balance = Column(NUMERIC(19,4), primary_key=True)
    tradeID = Column(Integer, primary_key=True)
    symbol = Column(VARCHAR(21), index=True)
    underlyingSymbol = Column(VARCHAR(21), index=True)
    currency = Column(VARCHAR(21))
    assetCategory = Column(VARCHAR(21))
    fxRateToBase = Column(NUMERIC(19,4))
    description = Column(String)
    multiplier = Column(NUMERIC(19,4))
    settleDateTarget = Column(Date)
    transactionType = Column(String)
    exchange = Column(String)
    quantity = Column(NUMERIC(19,4))
    tradePrice = Column(NUMERIC(19,4))
    proceeds = Column(NUMERIC(19,4))
    taxes = Column(NUMERIC(19,4))
    commission = Column(NUMERIC(19,4))
    commissionCurrency = Column(VARCHAR(21))
    openCloseIndicator = Column(String)
    notes = Column(String)
    cost = Column(NUMERIC(19,4))
    fifoPnlRealized = Column(NUMERIC(19,4))
    mtmPnl = Column(NUMERIC(19,4))
    buySell = Column(String)
    netCash = Column(NUMERIC(19,4))
    orderType = Column(String)
    orderTime = Column(DateTime(timezone=True))

def add_timezone(date, time, locale='US/Eastern', fmt='%m/%d/%Y %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return dt.date().isoformat(), ' '.join([dt.time().isoformat(), tzone])

def gen_position_data(row, fieldmap, cashdesc=None):
    position_data = {}
    for field in row:
        try:
            if fieldmap[field] in ['qty', 'price', 'total_value']: 
                data = row[field].strip('$').replace(',','')
                position_data[fieldmap[field]] = data
            elif fieldmap[field]:           
                position_data[fieldmap[field]] = row[field]
        except KeyError:
            print 'There is no %s field in the fieldmap.' % field
    if 'cash' in position_data['symbol'].lower():
        position_data['symbol'] = 'USD'
        position_data['price'] = 1
        position_data['qty'] = position_data['total_value']
        if cashdesc: position_data['description'] = cashdesc
    return position_data

def get_id(account_info, session):
    db_acc = session.query(Account).filter_by(**account_info).first()
    if not db_acc:
        db_acc = Account(**account_info)
        session.add(db_acc)
        session.commit()
    return db_acc.id
