#!/usr/bin/python
from sqlalchemy import Column, ForeignKey, Integer, String, Time
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import CHAR, DATE, NUMERIC, VARCHAR

Base = declarative_base()

class Account(Base):
    __tablename__ = 'accounts'
    __table_args__ = {'schema':'portfolio'}
    id = Column(Integer, primary_key=True, unique=True)
    institution = Column(String, primary_key=True)
    account = Column(String, primary_key=True)
    positions = relationship('Position', backref='accounts')

class Position(Base):
    __tablename__ = 'positions'
    __table_args__ = {'schema':'portfolio'}
    id = Column(Integer, ForeignKey('accounts.id'), primary_key=True,
                index=True)
    date = Column(DATE, primary_key=True, index=True)
    time = Column(Time(timezone=True), primary_key=True, index=True)
    symbol = Column(VARCHAR(6), primary_key=True, index=True)
    description = Column(String)
    qty = Column(NUMERIC(17,4))
    price = Column(NUMERIC(16,3))
    change = Column(NUMERIC(16,3))
    total_value = Column(NUMERIC(16,3))

def gen_position(row, fieldmap):
    position = {}
    for field in row:
        if fieldmap[field]: position[fieldmap[field]] = row[field]
    return Position(**position)        
