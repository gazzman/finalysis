#!/usr/bin/python
import sys

from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

#from finalysis.account_orms import SCHEMA as position_schema
#from finalysis.research_orms import SCHEMA as data_schema
from finalysis.account_orms import Position
from finalysis.research_orms import (AssetAllocation,
                                     CountryAllocation,
                                     Fund,
                                     Holdings,
                                     Equity,
                                     FixedIncome,
                                     MktCapAllocation,
                                     RegionAllocation,
                                     SectorAllocation)

def weighted_value(weight, value):
    if weight: return weight * value / 100
    else: return 0

if __name__ == '__main__':
    db_name = sys.argv[1]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)
    metadata = MetaData()
    Session = sessionmaker(bind=engine)
    session = Session()

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
    max_timestamp = func.max(positions.columns.timestamp)
    order = [positions.columns.id, positions.columns.symbol]

    current_pos = session.query(max_timestamp, *pos_cols)\
                                                .group_by(*pos_cols).subquery()
    current_value = session.query(func.sum(current_pos.columns.total_value))\
                                                                   .all()[0][0]
    current_cash = session.query(func.sum(current_pos.columns.total_value))\
                        .filter(current_pos.columns.symbol=='CASH').all()[0][0]

    sym_values = [x for x in session.query(current_pos.columns.symbol,
                                        current_pos.columns.total_value).all()]
    symbols = {}
    for (symbol, value) in sym_values:
        try: symbols[symbol] += value
        except KeyError: symbols[symbol] = value

    exclude = ['ticker', 'date']
    allocations = {}
    for table in allocation_tables:
        categories = [c for c in table.columns if c.name not in exclude]
        fieldnames = [c.name for c in categories]
        total_value = [0 for x in fieldnames]
        rows = session.query(table.columns.ticker, *categories)\
                        .filter(table.columns.ticker.in_(symbols.keys())).all()
        for row in rows:
            symbol = row[0]
            value = [weighted_value(x, symbols[symbol]) for x in row[1:]]
            total_value = [x + y for x, y in zip(total_value, value)]
        relative_value = [100*x/current_value for x in total_value]
        allocations[table.name] = zip(fieldnames, total_value, relative_value)
