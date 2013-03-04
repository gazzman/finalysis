#!/usr/bin/python
from datetime import datetime
import argparse
import sys
import lxml.etree as etree

from sqlalchemy import MetaData, Table
from sqlalchemy import alias, create_engine, func
from sqlalchemy.orm import sessionmaker

XML_STYLESHEET = '<?xml-stylesheet type="text/xsl" href="prices.xsl"?>'

def weighted_value(weight, value):
    if weight: return weight * value / 100
    else: return 0

def titlify(category_name):
    category_name = category_name.replace('_', ' ')
    category_name = category_name.replace('pct ', '')
    return category_name.title()


if __name__ == '__main__':
    def_tab = 'yahoo_daily_prices'
    def_schema = 'prices'
    def_to = datetime.now().date().isoformat()
    description = 'A script that analyzes portfolio positions and allocations.'
    initial_help = 'a text file of tickers and initial portfolio quantities'
    db_help = 'the name of the database where the price data is stored'
    tab_help = 'the name of the price data table. Default is %s' % def_tab
    schema_help = 'the schema. Default is %s' % def_schema
    from_help = '%%Y-%%m-%%d date from which to start'
    to_help = '%%Y-%%m-%%d date from which to end. Default is today.'

    p = argparse.ArgumentParser(description=description)
    p.add_argument('initial_position', help=initial_help)
    p.add_argument('db_name', help=db_help)
    p.add_argument('from_date', help=from_help)
    p.add_argument('--to_date', help=to_help, default=def_to)
    p.add_argument('--tablename', help=tab_help, default=def_tab)
    p.add_argument('--schema', help=schema_help, default=def_schema)
    args = p.parse_args()
    portfolio_schema = args.schema

    report = etree.Element('report')
    fromdate = etree.SubElement(report, 'timestamp', {'type': 'from'})
    fromdate.text = args.from_date
    todate = etree.SubElement(report, 'timestamp', {'type': 'to'})
    todate.text = args.to_date

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + args.db_name
    engine = create_engine(dburl)
    metadata = MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Reflect tables
    prices = Table(args.tablename, metadata, autoload=True, 
                   autoload_with=engine, schema=args.schema)

    ticker_col = prices.columns.ticker
    date_col = prices.columns.date

    # Read the positions file
    with open(args.initial_position, 'r') as positions:
        lines = positions.read().strip().split('\n')
        pairs = [x.strip().split() for x in lines]
        tickers, qtys = zip(*pairs)

    price_cols = []
    date_cols = []
    for ticker, qty in zip(tickers, qtys):
        subquery = session.query(prices)\
                          .filter(ticker_col==ticker)\
                          .subquery()
        price_cols += [subquery.columns.adj_close * float(qty)]
        date_cols += [subquery.columns.date]

    # Ensure dates all match
    main_query = session.query(date_cols[0], sum(price_cols), *price_cols)\
                        .filter(date_cols[0]>=args.from_date)\
                        .filter(date_cols[0]<=args.to_date)\
                        .order_by(date_cols[0])

    for i in range(1,len(date_cols)):
        main_query = main_query.filter(date_cols[i]==date_cols[i-1])

    # Get the prices, weights, and returns
    date_prices = main_query.all()
    date_weights = [[x[0]] + [a/x[1] for a in x[1:]] for x in date_prices]
    date_returns = [[x[0]] + [a/b - 1 for (a, b) in zip(x[1:], y[1:])]
                    for (x, y) in zip(date_prices[1:], date_prices[:-1])]

    for p, w, r in zip(date_prices[1:], date_weights[1:], date_returns):
        datapoint = etree.SubElement(report, 'datapoint')
        date = etree.SubElement(datapoint, 'timestamp')
        date.text = r[0].isoformat()
        overall = etree.SubElement(datapoint, 'overall')
        price = etree.SubElement(overall, 'dollar_value')
        price.text = '%16.4f' % p[1]
        weight = etree.SubElement(overall, 'proportion', {'type': 'weight'})
        weight.text = '%16.4f' % w[1]
        ret = etree.SubElement(overall, 'proportion', {'type': 'return'})
        ret.text = '%16.4f' % r[1]
        for i in range(0, len(tickers)):
            ticker = etree.SubElement(overall, 'ticker')
            ticker.text = tickers[i]
            qty = etree.SubElement(ticker, 'quantity')
            qty.text = qtys[i]
            price = etree.SubElement(ticker, 'dollar_value')
            price.text = '%16.4f' % p[i+2]
            weight = etree.SubElement(ticker, 'proportion', {'type': 'weight'})
            weight.text = '%16.4f' % w[i+2]
            ret = etree.SubElement(ticker, 'proportion', {'type': 'return'})
            ret.text = '%16.4f' % r[i+2]

    # Store data in xml file
    with open('portfolio_prices_%s_%s.xml' % (args.from_date, args.to_date),
              'w') as xmlfile:
        xmlfile.write('%s\n' % XML_STYLESHEET)
        xmlfile.write(etree.tostring(report, pretty_print=True))
