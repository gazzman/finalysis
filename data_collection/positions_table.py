#!/usr/bin/python
from sqlalchemy import Column, ForeignKey, Integer, String, Time
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import (CHAR, DATE, INTEGER, 
                                            NUMERIC, VARCHAR)

Base = declarative_base()

class Account(Base):
    __tablename__ = 'accounts'
    # Identifying fields    
    id = Column(Integer, primary_key=True, unique=True)
    institution = Column(String, primary_key=True)
    account = Column(String, primary_key=True)

    positions = relationship('Position', backref='accounts')
    

class Position(Base):
    __tablename__ = 'positions'
    id = Column(Integer, ForeignKey('accounts.id'), primary_key=True,
                index=True)
    date = Column(DATE, primary_key=True, index=True)
    time = Column(Time(timezone=True), primary_key=True, index=True)

    # Schwab fields
    symbol = Column(VARCHAR(6), primary_key=True, index=True)
    name = Column(String)
    quantity = Column(NUMERIC(17,4))
    price = Column(NUMERIC(16,3))
    change = Column(NUMERIC(16,3))
    market_value = Column(NUMERIC(16,3))
    day_change_dollar = Column(NUMERIC(16,3))
    day_change_pct = Column(NUMERIC(9,2))
    reinvest_dividends = Column(VARCHAR(3))
    reinvest_capital_gain = Column(VARCHAR(3))
    pct_of_account = Column(NUMERIC(9,2))
    security_type = Column(String)

