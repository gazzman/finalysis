#!/usr/bin/python
from datetime import datetime
from decimal import Decimal
try: from collections import OrderedDict                # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
import glob
import os
import signal
import sys

from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.dialects.postgresql import NUMERIC
from sqlalchemy.schema import CreateSchema
from finalysis.plot_prices.butterfly_prices import gen_strike_intervals

Base = declarative_base()

class TableMixin(object):
    """A Mixin for an SQLAlchemy ORM whose classname is the tablename"""
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    time = Column(DateTime(timezone=True), primary_key=True)
    spot_bid = Column(NUMERIC(8,3))
    spot_ask = Column(NUMERIC(8,3))
    spot_mid = Column(NUMERIC(8,3))

def gen_prices_table(symbol, expiry, start, end, increment, schema=None):
    start *= 1000
    end *= 1000
    increment *=1000
    tablename = '%s_%s_%08i_%08i_%08i' % (symbol, expiry, start, end, increment)
    method_dict = OrderedDict()
    for i in range(0, (end - start)/increment):
        method_dict['call_bid_fly%i' % i] = Column(NUMERIC(8,3))
        method_dict['call_ask_fly%i' % i] = Column(NUMERIC(8,3))
        method_dict['call_mid_fly%i' % i] = Column(NUMERIC(8,3))
        method_dict['put_bid_fly%i' % i] = Column(NUMERIC(8,3))
        method_dict['put_ask_fly%i' % i] = Column(NUMERIC(8,3))
        method_dict['put_mid_fly%i' % i] = Column(NUMERIC(8,3))
    TableClass = type(tablename, (Base, TableMixin), method_dict)
    if schema is not None:
        TableClass.__table__.schema = schema
    return TableClass

def read_file(filename, stub):
    f = open(filename, 'r')
    lines = [x.strip() for x in f.read().split('\n') if x.strip() != '']
    lines = [x.split()[1:] for x in lines if x[0] != '#']
    data = {}
    count = 0
    for line in lines:
        data['%s_bid_fly%i' % (stub, count)] = s[0]
        data['%s_ask_fly%i' % (stub, count)] = s[1]
        data['%s_mid_fly%i' % (stub, count)] = s[2]
        count += 1
    f.close()
    return data

if __name__ == "__main__":
    ''' format of the symbols_file is:

        ticker_symbol expiry_date starting_strike ending_strike increment

    '''
    symbols_file = sys.argv[1]
    db_name = sys.argv[2]
    schema = 'butterflies'
    f = open(symbols_file, 'r')
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)
    engine.execute(CreateSchema(schema))
    Session = sessionmaker(bind=engine)
    session = Session()

    for line in f:
        symbol, expiry, start, end, increment = line.split()
        symExp = (symbol, expiry)
        if not os.path.exists('_'.join(symExp)):
            raise Exception("Can't find the %s_%s directory" % (symbol, expiry))
        undfnames = glob.glob('%s_%s/underlying_*.dat' % (symbol, expiry))
        callfnames = glob.glob('%s_%s/buttercalls_*.dat' % (symbol, expiry))
        putfnames = glob.glob('%s_%s/butterputs_*.dat' % (symbol, expiry))
        if not (len(callfnames) == len(putfnames) == len(undfnames)):
            raise Exception("Information is missing")
        start = Decimal(start)
        end = Decimal(end)
        increment = Decimal(increment)
        T = gen_prices_table(symbol, expiry, start, end, increment, schema=schema)
        Base.metadata.create_all(engine)
        undfnames.sort()
        callfnames.sort()
        putfnames.sort()
        fnames = zip(undfnames, callfnames, putfnames)
        for fname in fnames:
            dt = fname[0].split('.dat')[0].split('underlying_')[-1]
            try:
                dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:                
                dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
            row = {'time': dt}
            f = open(fname[0], 'r')
            s = [x.strip() for x in f.read().split('\n') if x.strip() != '']
            s = [x.split()[0] for x in s if x[0] != '#']
            f.close()
            row['spot_bid'] = s[0]
            row['spot_ask'] = s[1]
            row['spot_mid'] = s[2]
            row.update(read_file(fname[1], 'call'))
            row.update(read_file(fname[2], 'put'))
            session.add(T(**row))
    session.commit()
