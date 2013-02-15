#!/usr/bin/python
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
from datetime import datetime
#import logging
import sys
import lxml.etree as etree

from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from finalysis.account_orms import Position
from finalysis.research_orms import (AssetAllocation,
                                     CountryAllocation,
                                     Fund,
                                     Holdings,
                                     Equity,
                                     FixedIncome,
                                     MktCapAllocation,
#                                     RegionAllocation,
                                     SectorAllocation)

XML_STYLESHEET = '<?xml-stylesheet type="text/xsl" href="allocations.xsl"?>'

#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def weighted_value(weight, value):
    if weight: return weight * value / 100
    else: return 0

def titlify(category_name):
    category_name = category_name.replace('_', ' ')
    category_name = category_name.replace('pct ', '')
    return category_name.title()

if __name__ == '__main__':
    db_name = sys.argv[1]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)
    metadata = MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()

    report = etree.Element('report')
    # Reflect tables
    positions = Table(Position.__tablename__, metadata, autoload=True, 
                     autoload_with=engine)

    allocation_tables = [Table(locals()[x].__tablename__, metadata, 
                         autoload=True, autoload_with=engine)
                                 for x in locals().keys() if 'Allocation' in x]

#    fund = Table(Fund.__tablename__, metadata, autoload=True, 
#                 autoload_with=engine)
#    holdings = Table(Holdings.__tablename__, metadata, autoload=True, 
#                     autoload_with=engine)
#    equity = Table(Equity.__tablename__, metadata, autoload=True, 
#                   autoload_with=engine)
#    fixed_income = Table(FixedIncome.__tablename__, metadata, autoload=True, 
#                         autoload_with=engine)

    # Current position query
    pos_cols = ['id', 'symbol', 'qty', 'price', 'total_value']
    pos_cols = [c for c in positions.columns if c.name in pos_cols]
    distinct_cols = ['id', 'symbol']
    distinct_cols = [c for c in positions.columns if c.name in distinct_cols]
    max_timestamp = func.max(positions.columns.timestamp)
    order = [positions.columns.id, positions.columns.symbol]

    current_pos = session.query(max_timestamp, *pos_cols)\
                         .distinct(*distinct_cols)\
                         .group_by(*pos_cols)\
                         .subquery()
    current_value = session.query(func.sum(current_pos.columns\
                                                     .total_value)).all()[0][0]
    portfolio_value = etree.SubElement(report, 'portfolio_value')
    portfolio_value.text = '%16.4f' % current_value
    current_cash = etree.SubElement(report, 'total_cash')
    current_cash.text = '%16.4f' % session.query(func.sum(current_pos.columns\
          .total_value)).filter(current_pos.columns.symbol=='USD').all()[0][0]

    sym_values = [x for x in session.query(current_pos.columns.symbol,
                                        current_pos.columns.total_value).all()]
    symbols = {}
    for (symbol, value) in sym_values:
        try: symbols[symbol] += value
        except KeyError: symbols[symbol] = value

    exclude = ['ticker', 'date']
    allocation_reports = etree.SubElement(report, 'allocation_reports')
    for table in allocation_tables:
        max_date = func.max(table.columns.date)
        categories = [c for c in table.columns if c.name not in exclude]
        fieldnames = [c.name for c in categories]
        total_value = [0 for x in fieldnames]
        rows = session.query(max_date, table.columns.ticker, *categories)\
                        .filter(table.columns.ticker.in_(symbols.keys()))\
                        .group_by(table.columns.ticker, *categories).all()
        for row in rows:
            symbol = row[1]
            value = [weighted_value(x, symbols[symbol]) for x in row[2:]]
            total_value = [x + y for x, y in zip(total_value, value)]
        relative_value = [x/current_value for x in total_value]
        allocation = etree.SubElement(allocation_reports, 'allocation_report',
                                      {'title': titlify(table.name)})
        for row in zip(fieldnames, total_value, relative_value):
            category = etree.SubElement(allocation, 'category')
            name = etree.SubElement(category, 'name')
            name.text = titlify(row[0])
            dollar = etree.SubElement(category, 'dollar_amount')
            dollar.text = '%16.4f' % row[1]
            proportion = etree.SubElement(category, 'wealth_proportion')
            proportion.text = '%16.4f' % row[2]

    date = datetime.now().date().isoformat()
    with open('portfolio_allocations_%s.xml' % date, 'w') as xmlfile:
        xmlfile.write('%s\n' % XML_STYLESHEET)
        xmlfile.write(etree.tostring(report, pretty_print=True))
