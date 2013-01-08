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

    # Schwab and Fidelity fields
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

    # Scottrade 
    acct_type = Column(String)
    annual_dividend = Column(NUMERIC(16,3))
    ask = Column(NUMERIC(16,3))
    ask_exchange = Column(String)
    ask_size = Column(INTEGER)
    average_daily_volume_100 = Column(INTEGER)
    average_daily_volume_22 = Column(INTEGER)
    beta = Column(NUMERIC(9,3))
    bid = Column(NUMERIC(16,3))
    bid_exchange = Column(String)
    bid_size = Column(INTEGER)
    cur_qtr_est_eps = Column(NUMERIC(9,2))
    cur_year_est_eps = Column(NUMERIC(9,2))
    currency = Column(VARCHAR(3))
    cusip = Column(INTEGER)
    div_pay_date = Column(DATE)
    dividend_yield = Column(NUMERIC(9,2))
    est_report_date = Column(DATE)
    growth_5_year = Column(NUMERIC(16,3))
    high = Column(NUMERIC(16,3))
    high_52wk = Column(NUMERIC(16,3))
    high_52wk_date = Column(DATE)
    last_12_month_eps = Column(NUMERIC(9,2))
    last_dividend = Column(NUMERIC(16,3))
    last_ex_div_date = Column(DATE)
    low = Column(NUMERIC(16,3))
    low_52wk = Column(NUMERIC(16,3))
    low_52wk_date = Column(DATE)
    month_close_price = Column(NUMERIC(16,3))
    moving_average_100 = Column(NUMERIC(16,3))
    moving_average_21 = Column(NUMERIC(16,3))
    moving_average_50 = Column(NUMERIC(16,3))
    moving_average_9 = Column(NUMERIC(16,3))
    nav = Column(NUMERIC(16,3))
    next_ex_div_date = Column(DATE)
    next_qtr_est_eps = Column(NUMERIC(9,2))
    next_year_est_eps = Column(NUMERIC(9,2))
    open = Column(NUMERIC(16,3))
    open_interest = Column(INTEGER)
    p_e_ratio = Column(NUMERIC(16,3))
    prev_close = Column(NUMERIC(16,3))
    primary_exchange = Column(String)
    qtr_close_price = Column(NUMERIC(16,3))
    ror_12month = Column(NUMERIC(11,4))
    time_traded = Column(Time)
    volatility_20day = Column(NUMERIC(11,4))
    volume = Column(INTEGER)
    week_close_price = Column(NUMERIC(16,3))
    year_close_price = Column(NUMERIC(16,3))

    # InteractiveBrokers
    multiplier = Column(INTEGER)
