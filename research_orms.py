#!/usr/bin/python
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Column, ForeignKey, Integer, String, Date
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declared_attr, declarative_base
from sqlalchemy.dialects.postgresql import CHAR, NUMERIC, VARCHAR

SCHEMA = 'fund_research'

Base = declarative_base()

class Ticker(Base):
    __tablename__ = 'tickers'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), primary_key=True)
    type = Column(String)
    description = Column(String)

    asset = relationship('AssetAllocation', backref='tickers')
    country = relationship('CountryAllocation', backref='tickers')
    mkt_cap = relationship('MktCapAllocation', backref='tickers')
    region = relationship('RegionAllocation', backref='tickers')
    sector = relationship('SectorAllocation', backref='tickers')

    fund = relationship('Fund', backref='tickers')
    holdings = relationship('Holdings', backref='tickers')
    equity = relationship('Equity', backref='tickers')
    fixed_income = relationship('FixedIncome', backref='tickers')

class AssetAllocation(Base):
    __tablename__ = 'asset_allocation'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA), 
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    pct_long_bond = Column(NUMERIC(19,4))
    pct_long_cash = Column(NUMERIC(19,4))
    pct_long_equity = Column(NUMERIC(19,4))
    pct_long_other = Column(NUMERIC(19,4))
    pct_short_bond = Column(NUMERIC(19,4))
    pct_short_cash = Column(NUMERIC(19,4))
    pct_short_equity = Column(NUMERIC(19,4))
    pct_short_other = Column(NUMERIC(19,4))

class CountryAllocation(Base):
    __tablename__ = 'country_allocation'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA), 
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    argentina = Column(NUMERIC(19,4))
    australia = Column(NUMERIC(19,4))
    austria = Column(NUMERIC(19,4))
    belgium = Column(NUMERIC(19,4))
    bermuda = Column(NUMERIC(19,4))
    brazil = Column(NUMERIC(19,4))
    canada = Column(NUMERIC(19,4))
    cayman_islands = Column(NUMERIC(19,4))
    chile = Column(NUMERIC(19,4))
    china = Column(NUMERIC(19,4))
    colombia = Column(NUMERIC(19,4))
    cyprus = Column(NUMERIC(19,4))
    czech_republic = Column(NUMERIC(19,4))
    denmark = Column(NUMERIC(19,4))
    finland = Column(NUMERIC(19,4))
    france = Column(NUMERIC(19,4))
    germany = Column(NUMERIC(19,4))
    ghana = Column(NUMERIC(19,4))
    hong_kong = Column(NUMERIC(19,4))
    india = Column(NUMERIC(19,4))
    indonesia = Column(NUMERIC(19,4))
    ireland = Column(NUMERIC(19,4))
    israel = Column(NUMERIC(19,4))
    italy = Column(NUMERIC(19,4))
    japan = Column(NUMERIC(19,4))
    luxembourg = Column(NUMERIC(19,4))
    macao = Column(NUMERIC(19,4))
    malaysia = Column(NUMERIC(19,4))
    mexico = Column(NUMERIC(19,4))
    monaco = Column(NUMERIC(19,4))
    netherlands = Column(NUMERIC(19,4))
    new_zealand = Column(NUMERIC(19,4))
    norway = Column(NUMERIC(19,4))
    panama = Column(NUMERIC(19,4))
    peru = Column(NUMERIC(19,4))
    philippines = Column(NUMERIC(19,4))
    poland = Column(NUMERIC(19,4))
    puerto_rico = Column(NUMERIC(19,4))
    russia = Column(NUMERIC(19,4))
    singapore = Column(NUMERIC(19,4))
    south_africa = Column(NUMERIC(19,4))
    south_korea = Column(NUMERIC(19,4))
    spain = Column(NUMERIC(19,4))
    sweden = Column(NUMERIC(19,4))
    switzerland = Column(NUMERIC(19,4))
    taiwan = Column(NUMERIC(19,4))
    thailand = Column(NUMERIC(19,4))
    united_kingdom = Column(NUMERIC(19,4))
    united_states = Column(NUMERIC(19,4))

class MktCapAllocation(Base):
    __tablename__ = 'mkt_cap_allocation'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA),
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    giant_cap = Column(NUMERIC(19,4))
    large_cap = Column(NUMERIC(19,4))
    micro_cap = Column(NUMERIC(19,4))
    mid_cap = Column(NUMERIC(19,4))
    small_cap = Column(NUMERIC(19,4))

class RegionAllocation(Base):
    __tablename__ = 'region_allocation'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA),
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    africa = Column(NUMERIC(19,4))
    asia_developed = Column(NUMERIC(19,4))
    asia_emerging = Column(NUMERIC(19,4))
    australasia = Column(NUMERIC(19,4))
    canada = Column(NUMERIC(19,4))
    europe_emerging = Column(NUMERIC(19,4))
    europe_ex_euro = Column(NUMERIC(19,4))
    eurozone = Column(NUMERIC(19,4))
    japan = Column(NUMERIC(19,4))
    latin_america = Column(NUMERIC(19,4))
    middle_east = Column(NUMERIC(19,4))
    united_kingdom = Column(NUMERIC(19,4))
    united_states = Column(NUMERIC(19,4))

class SectorAllocation(Base):
    __tablename__ = 'sector_allocation'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA),
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    cash_equivalent = Column(NUMERIC(19,4))
    commingled_fund = Column(NUMERIC(19,4))
    consumer_discretionary = Column(NUMERIC(19,4))
    consumer_staples = Column(NUMERIC(19,4))
    energy = Column(NUMERIC(19,4))
    financials = Column(NUMERIC(19,4))
    health_care = Column(NUMERIC(19,4))
    industrials = Column(NUMERIC(19,4))
    information_technology = Column(NUMERIC(19,4))
    materials = Column(NUMERIC(19,4))
    telecommunication_services = Column(NUMERIC(19,4))
    utilities = Column(NUMERIC(19,4))

class Fund(Base):
    __tablename__ = 'fund'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA), 
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    _1_month_change = Column(NUMERIC(19,4))
    _1_week_change = Column(NUMERIC(19,4))
    _3_month_change = Column(NUMERIC(19,4))
    _52_week_range = Column(String)
    actively_managed = Column(String)
    avg_volume_10_day = Column(Integer)
    capital_gain_ex_date = Column(Date)
    category_average = Column(NUMERIC(19,4))
    change = Column(String)
    closing_nav = Column(NUMERIC(19,4))
    days_range = Column(String)
    details_date = Column(Date)
    distribution_frequency = Column(String)
    distribution_yield_ttm_help = Column(NUMERIC(19,4))
    diversified_portfolio = Column(String)
    fund_company = Column(String)
    fund_type = Column(String)
    gross_expense_ratio = Column(NUMERIC(19,4))
    inception = Column(Date)
    index_fund = Column(String)
    inverse_fund = Column(String)
    legal_structure_help = Column(String)
    leveraged_etf = Column(String)
    leveraged_factor_help = Column(NUMERIC(19,4))
    management = Column(String)
    manager_tenure = Column(Integer)
    morningstar_category = Column(String)
    most_recent_capital_gain = Column(NUMERIC(19,4))
    most_recent_distribution = Column(NUMERIC(19,4))
    net_expense_ratio = Column(NUMERIC(19,4))
    next_dividend_payment = Column(NUMERIC(19,4))
    next_pay_date = Column(Date)
    portfolio_turnover = Column(NUMERIC(19,4))
    premium_to_discount = Column(NUMERIC(19,4))
    previous_close = Column(NUMERIC(19,4))
    previous_dividend_payment = Column(NUMERIC(19,4))
    previous_ex_date = Column(Date)
    previous_pay_date = Column(Date)
    prospectus_benchmark = Column(String)
    put_to_call_ratio_1_day = Column(NUMERIC(19,4))
    put_to_call_ratio_30_day = Column(NUMERIC(19,4))
    sec_yield_30_day = Column(NUMERIC(19,4))
    socially_conscious = Column(String)
    todays_open = Column(NUMERIC(19,4))
    total_assets = Column(NUMERIC(19,4))
    total_holdings = Column(Integer)

class Holdings(Base):
    __tablename__ = 'holdings'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA),
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    description_1 = Column(String)
    description_2 = Column(String)
    description_3 = Column(String)
    description_4 = Column(String)
    description_5 = Column(String)
    description_6 = Column(String)
    description_7 = Column(String)
    description_8 = Column(String)
    description_9 = Column(String)
    description_10 = Column(String)
    pct_of_net_assets_1 = Column(NUMERIC(19,4))
    pct_of_net_assets_2 = Column(NUMERIC(19,4))
    pct_of_net_assets_3 = Column(NUMERIC(19,4))
    pct_of_net_assets_4 = Column(NUMERIC(19,4))
    pct_of_net_assets_5 = Column(NUMERIC(19,4))
    pct_of_net_assets_6 = Column(NUMERIC(19,4))
    pct_of_net_assets_7 = Column(NUMERIC(19,4))
    pct_of_net_assets_8 = Column(NUMERIC(19,4))
    pct_of_net_assets_9 = Column(NUMERIC(19,4))
    pct_of_net_assets_10 = Column(NUMERIC(19,4))
    sector_1 = Column(String)
    sector_2 = Column(String)
    sector_3 = Column(String)
    sector_4 = Column(String)
    sector_5 = Column(String)
    sector_6 = Column(String)
    sector_7 = Column(String)
    sector_8 = Column(String)
    sector_9 = Column(String)
    sector_10 = Column(String)
    sub_industry_1 = Column(String)
    sub_industry_2 = Column(String)
    sub_industry_3 = Column(String)
    sub_industry_4 = Column(String)
    sub_industry_5 = Column(String)
    sub_industry_6 = Column(String)
    sub_industry_7 = Column(String)
    sub_industry_8 = Column(String)
    sub_industry_9 = Column(String)
    sub_industry_10 = Column(String)
    symbol_1 = Column(VARCHAR(21))
    symbol_2 = Column(VARCHAR(21))
    symbol_3 = Column(VARCHAR(21))
    symbol_4 = Column(VARCHAR(21))
    symbol_5 = Column(VARCHAR(21))
    symbol_6 = Column(VARCHAR(21))
    symbol_7 = Column(VARCHAR(21))
    symbol_8 = Column(VARCHAR(21))
    symbol_9 = Column(VARCHAR(21))
    symbol_10 = Column(VARCHAR(21))
    value_1 = Column(NUMERIC(19,4))
    value_2 = Column(NUMERIC(19,4))
    value_3 = Column(NUMERIC(19,4))
    value_4 = Column(NUMERIC(19,4))
    value_5 = Column(NUMERIC(19,4))
    value_6 = Column(NUMERIC(19,4))
    value_7 = Column(NUMERIC(19,4))
    value_8 = Column(NUMERIC(19,4))
    value_9 = Column(NUMERIC(19,4))
    value_10 = Column(NUMERIC(19,4))
    ytdpct_1 = Column(NUMERIC(19,4))
    ytdpct_2 = Column(NUMERIC(19,4))
    ytdpct_3 = Column(NUMERIC(19,4))
    ytdpct_4 = Column(NUMERIC(19,4))
    ytdpct_5 = Column(NUMERIC(19,4))
    ytdpct_6 = Column(NUMERIC(19,4))
    ytdpct_7 = Column(NUMERIC(19,4))
    ytdpct_8 = Column(NUMERIC(19,4))
    ytdpct_9 = Column(NUMERIC(19,4))
    ytdpct_10 = Column(NUMERIC(19,4))

class Equity(Base):
    __tablename__ = 'equity'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA), 
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    _52_week_range = Column(String)
    _52_week_range_1 = Column(String)
    _52_week_range_2 = Column(String)
    _52_week_range_3 = Column(String)
    _52_week_range_4 = Column(String)
    _52_week_range_5 = Column(String)
    annual_yield = Column(NUMERIC(19,4))
    average_volume_10_day = Column(Integer)
    beta = Column(NUMERIC(19,4))
    book_value_to_share_mrq = Column(NUMERIC(19,4))
    book_values_growth_pct = Column(NUMERIC(19,4))
    cash_as_percent_of_total_assets_pct_mrq = Column(NUMERIC(19,4))
    cash_flow_growth_pct = Column(NUMERIC(19,4))
    cash_flow_margin_pct = Column(NUMERIC(19,4))
    cash_flow_per_share = Column(NUMERIC(19,4))
    cash_to_share_mrq = Column(NUMERIC(19,4))
    current_ratio = Column(NUMERIC(19,4))
    days_range = Column(String)
    debt_to_capital = Column(NUMERIC(19,4))
    debt_to_equity_mrfy = Column(NUMERIC(19,4))
    dividend_payout_ratio_pct = Column(NUMERIC(19,4))
    dividend_yield_pct = Column(NUMERIC(19,4))
    dividend_yield_pct = Column(NUMERIC(19,4))
    earnings_per_share = Column(NUMERIC(19,4))
    enterprise_value = Column(NUMERIC(19,4))
    eps_change_pct_year_over_year_mrq = Column(NUMERIC(19,4))
    eps_date = Column(Date)
    gross_profit_margin_pct = Column(NUMERIC(19,4))
    historical_earnings_pct = Column(NUMERIC(19,4))
    industry = Column(String)
    long_term_debt = Column(NUMERIC(19,4))
    long_term_earnings_pct = Column(NUMERIC(19,4))
    market_capitalization = Column(NUMERIC(19,4))
    morningstar_category_1 = Column(String)
    morningstar_category_2 = Column(String)
    morningstar_category_3 = Column(String)
    morningstar_category_4 = Column(String)
    morningstar_category_5 = Column(String)
    name_1 = Column(String)
    name_2 = Column(String)
    name_3 = Column(String)
    name_4 = Column(String)
    name_5 = Column(String)
    net_profit_margin_pct = Column(NUMERIC(19,4))
    next_ex_date = Column(Date)
    next_pay_date = Column(Date)
    operating_profit_margin_pct = Column(NUMERIC(19,4))
    pct_of_fund_1 = Column(NUMERIC(19,4))
    pct_of_fund_2 = Column(NUMERIC(19,4))
    pct_of_fund_3 = Column(NUMERIC(19,4))
    pct_of_fund_4 = Column(NUMERIC(19,4))
    pct_of_fund_5 = Column(NUMERIC(19,4))
    peer_symbol_1 = Column(VARCHAR(21))
    peer_symbol_2 = Column(VARCHAR(21))
    peer_symbol_3 = Column(VARCHAR(21))
    peer_symbol_4 = Column(VARCHAR(21))
    previous_close = Column(NUMERIC(19,4))
    previous_ex_date = Column(Date)
    previous_pay_date = Column(Date)
    price_to_book = Column(NUMERIC(19,4))
    price_to_cash_flow = Column(NUMERIC(19,4))
    price_to_cash_flow = Column(NUMERIC(19,4))
    price_to_earnings = Column(NUMERIC(19,4))
    price_to_earnings_growth_5yr_projected_growth = Column(NUMERIC(19,4))
    price_to_forecasted_earnings_fyf = Column(NUMERIC(19,4))
    price_to_prospective_earnings = Column(NUMERIC(19,4))
    price_to_sales = Column(NUMERIC(19,4))
    price_to_sales = Column(NUMERIC(19,4))
    price_to_tangible_book_value_mrq = Column(NUMERIC(19,4))
    put_to_call_ratio_1_day = Column(NUMERIC(19,4))
    put_to_call_ratio_30_day = Column(NUMERIC(19,4))
    quarterly_dividend = Column(NUMERIC(19,4))
    quick_ratio = Column(NUMERIC(19,4))
    return_on_assets_pct = Column(NUMERIC(19,4))
    return_on_equity_pct = Column(NUMERIC(19,4))
    return_on_investment_pct = Column(NUMERIC(19,4))
    sales_growth_pct = Column(NUMERIC(19,4))
    sales_growth_pct = Column(NUMERIC(19,4))
    sales_per_employee = Column(NUMERIC(19,4))
    sector = Column(String)
    semi_annual_dividend = Column(NUMERIC(19,4))
    shares_held_by_institutions = Column(NUMERIC(19,4))
    shares_outstanding = Column(NUMERIC(19,4))
    stats_date = Column(Date)
    sub_industry = Column(String)
    symbol_1 = Column(VARCHAR(21))
    symbol_2 = Column(VARCHAR(21))
    symbol_3 = Column(VARCHAR(21))
    symbol_4 = Column(VARCHAR(21))
    symbol_5 = Column(VARCHAR(21))
    tangible_book_value_to_share_mrq = Column(NUMERIC(19,4))
    todays_open = Column(NUMERIC(19,4))
    us_allocation_date = Column(Date)
    us_allocation_equity_pct_long_non_us = Column(NUMERIC(19,4))
    us_allocation_equity_pct_long_us = Column(NUMERIC(19,4))
    us_allocation_equity_pct_short_non_us = Column(NUMERIC(19,4))
    us_allocation_equity_pct_short_us = Column(NUMERIC(19,4))
    yearly_dividend = Column(NUMERIC(19,4))

class FixedIncome(Base):
    __tablename__ = 'fixed_income'
    __table_args__ = {'schema':SCHEMA}
    ticker = Column(VARCHAR(21), ForeignKey('%s.tickers.ticker' % SCHEMA), 
                    primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    a = Column(NUMERIC(19,4))
    aa = Column(NUMERIC(19,4))
    aaa = Column(NUMERIC(19,4))
    agency_mortgage_backed = Column(NUMERIC(19,4))
    asset_backed = Column(NUMERIC(19,4))
    average_effective_duration_years = Column(NUMERIC(19,4))
    average_effective_maturity_years = Column(NUMERIC(19,4))
    average_weighted_coupon = Column(NUMERIC(19,4))
    average_weighted_price = Column(NUMERIC(19,4))
    b = Column(NUMERIC(19,4))
    bank_loan = Column(NUMERIC(19,4))
    bb = Column(NUMERIC(19,4))
    bbb = Column(NUMERIC(19,4))
    below_b = Column(NUMERIC(19,4))
    cash_and_equivalents = Column(NUMERIC(19,4))
    commercial_mortgage_backed = Column(NUMERIC(19,4))
    convertible = Column(NUMERIC(19,4))
    corporate_bond = Column(NUMERIC(19,4))
    covered_bond = Column(NUMERIC(19,4))
    effective_duration_help = Column(NUMERIC(19,4))
    future_to_forward = Column(NUMERIC(19,4))
    government = Column(NUMERIC(19,4))
    government_related = Column(NUMERIC(19,4))
    maturity_date = Column(Date)
    municipal_tax_exempt = Column(NUMERIC(19,4))
    municipal_taxable = Column(NUMERIC(19,4))
    non_agency_residential_mortgage_backed = Column(NUMERIC(19,4))
    not_rated = Column(NUMERIC(19,4))
    preferred_stock = Column(NUMERIC(19,4))
    range_1_3_years = Column(NUMERIC(19,4))
    range_10_15_years = Column(NUMERIC(19,4))
    range_15_20_years = Column(NUMERIC(19,4))
    range_20_30_years = Column(NUMERIC(19,4))
    range_3_5_years = Column(NUMERIC(19,4))
    range_5_7_years = Column(NUMERIC(19,4))
    range_7_10_years = Column(NUMERIC(19,4))
    range_over_30_years = Column(NUMERIC(19,4))
    ratings_date = Column(Date)
    sectors_date = Column(Date)
    standard_deviation_3yr_help = Column(NUMERIC(19,4))
    stats_date = Column(Date)
    swap = Column(NUMERIC(19,4))
    us_allocation_date = Column(Date)
    us_allocation_fixed_income_pct_long_non_us = Column(NUMERIC(19,4))
    us_allocation_fixed_income_pct_long_us = Column(NUMERIC(19,4))
    us_allocation_fixed_income_pct_short_non_us = Column(NUMERIC(19,4))
    us_allocation_fixed_income_pct_short_us = Column(NUMERIC(19,4))
    weighted_average_coupon_help = Column(NUMERIC(19,4))
    weighted_average_maturity_help = Column(NUMERIC(19,4))
