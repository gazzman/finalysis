#!/usr/bin/python
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
from datetime import datetime
#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
import argparse
import sys
import lxml.etree as etree

from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from finalysis.account_orms import Position
from finalysis.research_orms import (Ticker,
                                     AssetAllocation,
                                     CountryAllocation,
                                     Fund,
                                     Holdings,
                                     Equity,
                                     FixedIncome,
                                     MktCapAllocation,
#                                     RegionAllocation,
                                     SectorAllocation,
                                     SCHEMA)

XML_STYLESHEET = '<?xml-stylesheet type="text/xsl" href="allocations.xsl"?>'


def weighted_value(weight, value):
    if weight: return weight * value / 100
    else: return 0

def titlify(category_name):
    category_name = category_name.replace('_', ' ')
    category_name = category_name.replace('pct ', '')
    return category_name.title()

def report_fund(fund_type, parent_element, fund):
    max_date = func.max(fund.columns.date)
    rows = session.query(max_date, 
                        fund.columns.ticker, 
                        tickers.columns.description,
                        fund.columns.gross_expense_ratio,
                        fund.columns.net_expense_ratio)\
                        .distinct(fund.columns.gross_expense_ratio, fund.columns.ticker)\
                        .filter(fund.columns.ticker.in_(symbols.keys()))\
                        .filter(tickers.columns.ticker==fund.columns.ticker)\
                        .filter(tickers.columns.type==fund_type)\
                        .group_by(fund.columns.ticker,
                                  tickers.columns.description,
                                  fund.columns.gross_expense_ratio,
                                  fund.columns.net_expense_ratio)\
                        .order_by(fund.columns.gross_expense_ratio.desc(), fund.columns.ticker).all()
    avg_expense_ratio = 0
    total_fund_value = 0
    for row in rows:
        fund_lmnt = etree.SubElement(parent_element, 'fund')
        symbol, description, gross_exp, net_exp = row[1:]
        qty, value = symbols[symbol]
        if not net_exp: net_exp = gross_exp
        avg_expense_ratio += value*net_exp
        total_fund_value += value
        ticker = etree.SubElement(fund_lmnt, 'ticker')
        ticker.text = symbol
        desc_lmnt = etree.SubElement(fund_lmnt, 'description')
        desc_lmnt.text = description
        gross_exp_lmnt = etree.SubElement(fund_lmnt, 'expense_ratio', {'type': 'gross'})
        gross_exp_lmnt.text = '%16.4f' % (gross_exp/100)
        net_exp_lmnt = etree.SubElement(fund_lmnt, 'expense_ratio', {'type': 'net'})
        net_exp_lmnt.text = '%16.4f' % (net_exp/100)
        total_qty = etree.SubElement(fund_lmnt, 'quantity')
        total_qty.text = '%16.4f' % qty
        total_value = etree.SubElement(fund_lmnt, 'dollar_value')
        total_value.text = '%16.4f' % value
        proportion = etree.SubElement(fund_lmnt, 'proportion')
        proportion.text = '%16.4f' % (value/current_value)
    avg_expense_ratio /= total_fund_value*100
    avg_exp_lmnt = etree.SubElement(parent_element, 'expense_ratio')
    avg_exp_lmnt.text = '%16.4f' % avg_expense_ratio
    total_fund_lmnt = etree.SubElement(parent_element, 'dollar_value')
    total_fund_lmnt.text = '%16.4f' % total_fund_value
    fund_proportion = etree.SubElement(parent_element, 'proportion')
    fund_proportion.text = '%16.4f' % (total_fund_value/current_value)    
    return avg_expense_ratio, total_fund_value

if __name__ == '__main__':
    default_schema='portfolio'
    description = 'A script that analyzes portfolio positions and allocations.'
    p = argparse.ArgumentParser(description=description)
    p.add_argument('db_name', type=str, help='name of the postgresql database')
    p.add_argument('--schema', default=default_schema,
                   help="positions table schema; default is '%s'"
                         % default_schema)
    p.add_argument('--date', help='%%Y-%%m-%%d date from which to pull data')
    args = p.parse_args()
    portfolio_schema = args.schema

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + args.db_name
    engine = create_engine(dburl)
    metadata = MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()

    report = etree.Element('report')
    # Reflect tables
    positions = Table(Position.__tablename__, metadata, autoload=True, 
                     autoload_with=engine, schema=portfolio_schema)

    allocation_tables = [Table(locals()[x].__tablename__, metadata, 
                         autoload=True, autoload_with=engine, schema=SCHEMA)
                                 for x in locals().keys() if 'Allocation' in x]

    fund = Table(Fund.__tablename__, metadata, autoload=True, 
                 autoload_with=engine, schema=SCHEMA)
    tickers = Table(Ticker.__tablename__, metadata, autoload=True, 
                    autoload_with=engine, schema=SCHEMA)
#    holdings = Table(Holdings.__tablename__, metadata, autoload=True, 
#                     autoload_with=engine, schema=SCHEMA)
#    equity = Table(Equity.__tablename__, metadata, autoload=True, 
#                   autoload_with=engine, schema=SCHEMA)
#    fixed_income = Table(FixedIncome.__tablename__, metadata, autoload=True, 
#                         autoload_with=engine, schema=SCHEMA)

    # Current position query
    pos_cols = ['id', 'symbol', 'description', 'qty', 'price', 'total_value']
    pos_cols = [c for c in positions.columns if c.name in pos_cols]
    distinct_cols = ['id', 'symbol', 'description']
    distinct_cols = [c for c in positions.columns if c.name in distinct_cols]
    date_col = positions.columns.timestamp
    max_timestamp = func.max(date_col).label('timestamp')
    order = [positions.columns.id, positions.columns.symbol]

    if args.date: date = args.date
    else:
        current_pos = session.query(max_timestamp, *pos_cols)\
                                   .distinct(*distinct_cols)\
                                   .group_by(*pos_cols)\
                                   .subquery()  
        date = session.query(func.min(current_pos.columns.timestamp))\
                                .all()[0][0].date().isoformat()

    current_pos = session.query(max_timestamp, *pos_cols)\
                         .distinct(*distinct_cols)\
                         .group_by(*pos_cols)\
                         .filter(func.to_char(date_col, 'YYYY-MM-DD')==date)\
                         .subquery()

    timestamps = session.query(current_pos.columns.timestamp)\
                                .order_by(current_pos.columns.timestamp)\
                                .all()
    latest_timestamp = timestamps[-1][0]
    earliest_timestamp = timestamps[0][0]

    portfolio_date = etree.SubElement(report, 'date')
    portfolio_date.text = date
    early = etree.SubElement(report, 'timestamp', {'type': 'earliest'})
    early.text = earliest_timestamp.isoformat()
    late = etree.SubElement(report, 'timestamp', {'type': 'latest'})
    late.text = latest_timestamp.isoformat()

    # Generate portfolio value report
    total_prop = etree.SubElement(report, 'proportion', {'type': 'overall'})
    total_prop.text = '1'
    current_value = session.query(func.sum(current_pos.columns\
                                                     .total_value)).all()[0][0]
    current_cash = session.query(func.sum(current_pos.columns\
                         .total_value))\
                         .filter(current_pos.columns.symbol=='USD').all()[0][0]
    portfolio_value = etree.SubElement(report, 'dollar_value', {'type': 'overall'})
    portfolio_value.text = '%16.4f' % current_value
    cash_value = etree.SubElement(report, 'dollar_value', {'type': 'cash'})
    cash_value.text = '%16.4f' % current_cash
    cash_proportion = etree.SubElement(report, 'proportion', {'type': 'cash'})
    cash_proportion.text = '%16.4f' % (current_cash/current_value)
    sym_values = session.query(current_pos.columns.symbol,
                               current_pos.columns.qty,
                               current_pos.columns.total_value).all()

    symbols = {}
    for (symbol, qty, value) in sym_values:
        try: symbols[symbol] =  [ x + y for (x, y) in zip(symbols[symbol], 
                                                          (qty, value))]
        except KeyError: symbols[symbol] = (qty, value)

    # Generate Funds report
    try:
        etfs = etree.SubElement(report, 'etfs')
        etf_expense, etf_value = report_fund('ETF', etfs, fund)
    except ZeroDivisionError:
        etf_expense, etf_value = (0, 0)

    try:
        mfs = etree.SubElement(report, 'mfs')
        mf_expense, mf_value = report_fund('Mutual Fund', mfs, fund)
    except ZeroDivisionError:
        mf_expense, mf_value = (0, 0)

    overall_expense = etf_expense * etf_value
    overall_expense += mf_expense * mf_value
    overall_expense /= current_value
    overall_exp_lmnt = etree.SubElement(report, 'expense_ratio')
    overall_exp_lmnt.text = '%16.4f' % overall_expense

    # Generate Equities report
    equities = etree.SubElement(report, 'equities')
    rows = session.query(tickers.columns.ticker, tickers.columns.description)\
                        .filter(tickers.columns.ticker.in_(symbols.keys()))\
                        .filter(tickers.columns.type=='Stock').all()
    total_equity_value = 0
    for row in rows:
        equity = etree.SubElement(equities, 'equity')
        symbol, description = row
        qty, value = symbols[symbol]
        total_equity_value += value
        ticker = etree.SubElement(equity, 'ticker')
        ticker.text = symbol
        desc_lmnt = etree.SubElement(equity, 'description')
        desc_lmnt.text = description
        total_qty = etree.SubElement(equity, 'quantity')
        total_qty.text = '%16.4f' % qty
        total_value = etree.SubElement(equity, 'dollar_value')
        total_value.text = '%16.4f' % value
        proportion = etree.SubElement(equity, 'proportion')
        proportion.text = '%16.4f' % (value/current_value)
    total_equity_lmnt = etree.SubElement(equities, 'dollar_value')
    total_equity_lmnt.text = '%16.4f' % total_equity_value
    equity_proportion = etree.SubElement(equities, 'proportion')
    equity_proportion.text = '%16.4f' % (total_equity_value/current_value)

    # Generate Options report        
    options = etree.SubElement(report, 'options')
    option_symbols = [x for x in symbols.keys() if len(x) == 21]
    total_option_value = 0
    rows = session.query(positions.columns.symbol, positions.columns.description)\
                        .filter(positions.columns.symbol.in_(option_symbols)).all()
    for row in rows:
        option = etree.SubElement(options, 'option')
        symbol, description = row
        qty, value = symbols[symbol]
        total_option_value += value
        ticker = etree.SubElement(option, 'ticker')
        ticker.text = symbol
        desc_lmnt = etree.SubElement(option, 'description')
        desc_lmnt.text = description
        total_qty = etree.SubElement(option, 'quantity')
        total_qty.text = '%16.4f' % qty
        total_value = etree.SubElement(option, 'dollar_value')
        total_value.text = '%16.4f' % value
        proportion = etree.SubElement(option, 'proportion')
        proportion.text = '%16.4f' % (value/current_value)
    total_option_lmnt = etree.SubElement(options, 'dollar_value')
    total_option_lmnt.text = '%16.4f' % total_option_value
    option_proportion = etree.SubElement(options, 'proportion')
    option_proportion.text = '%16.4f' % (total_option_value/current_value)

    # Generate Allocation Reports
    exclude = ['ticker', 'date']
    allocation_reports = etree.SubElement(report, 'allocation_reports')
    for table in allocation_tables:
        max_date = func.max(table.columns.date)
        categories = [c for c in table.columns if c.name not in exclude]
        fieldnames = [c.name for c in categories]
        total_value = [0 for x in fieldnames]
        rows = session.query(max_date, table.columns.ticker, *categories)\
                        .distinct(table.columns.ticker)\
                        .filter(table.columns.ticker.in_(symbols.keys()))\
                        .group_by(table.columns.ticker, *categories).all()
        for row in rows:
            symbol = row[1]
            qty, value = symbols[symbol]
            value = [weighted_value(x, value) for x in row[2:]]
            total_value = [x + y for x, y in zip(total_value, value)]
        relative_value = [x/current_value for x in total_value]
        allocation = etree.SubElement(allocation_reports, 'allocation_report',
                                      {'title': titlify(table.name)})
        for row in zip(fieldnames, total_value, relative_value):
            category = etree.SubElement(allocation, 'category')
            name = etree.SubElement(category, 'name')
            name.text = titlify(row[0])
            dollar = etree.SubElement(category, 'dollar_value')
            dollar.text = '%16.4f' % row[1]
            proportion = etree.SubElement(category, 'proportion')
            proportion.text = '%16.4f' % row[2]

    # Store data in xml file
    with open('portfolio_allocations_%s.xml' % date, 'w') as xmlfile:
        xmlfile.write('%s\n' % XML_STYLESHEET)
        xmlfile.write(etree.tostring(report, pretty_print=True))
