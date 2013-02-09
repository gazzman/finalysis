#!/usr/bin/python
from decimal import Decimal

from sqlalchemy import Column, ForeignKey, Integer, String, Time
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import CHAR, DATE, NUMERIC, VARCHAR

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
    id = Column(Integer, ForeignKey('%s.accounts.id' % SCHEMA),
                primary_key=True, index=True)
    date = Column(DATE, primary_key=True, index=True)
    time = Column(Time(timezone=True), primary_key=True, index=True)
    symbol = Column(VARCHAR(6), primary_key=True, index=True)
    description = Column(String, primary_key=True, index=True)
    qty = Column(NUMERIC(17,4))
    price = Column(NUMERIC(16,3))
    total_value = Column(NUMERIC(16,3))

def gen_position_data(row, fieldmap):
    position_data = {}
    for field in row:
        if fieldmap[field] in ['qty', 'price', 'total_value']: 
            data = row[field].strip('$').replace(',','')
            position_data[fieldmap[field]] = data
        elif fieldmap[field]:           
            position_data[fieldmap[field]] = row[field]
    if 'cash' in position_data['symbol'].lower():
        position_data['symbol'] = 'CASH'
        position_data['price'] = 1
        position_data['qty'] = position_data['total_value']
    return position_data
