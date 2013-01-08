#!/usr/bin/python
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from StringIO import StringIO
import csv
import logging
import os.path
import sys
import time

from pytz import timezone
from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey, Integer, String, Time
from sqlalchemy.orm import backref, relationship, sessionmaker
from sqlalchemy.exc import IntegrityError
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
    
class AddDBMixin():
    def add_timezone(self, date, time, fmt='%Y-%m-%d %H:%M:%S',
                     locale='US/Eastern'):
        tz = timezone(locale)
        dt = ' '.join([date, time])
        dt = datetime.strptime(dt, fmt)
        tzone = tz.tzname(dt)
        return dt.date().isoformat(), ' '.join([dt.time().isoformat(), tzone])

    def seconds_elapsed(self, start, end):
        s = (end - start).seconds
        m = (end - start).microseconds
        return round(s + m/1000000.0,3)

    def fix_header(self, header):
        header = header.lower().strip()
        header = '_'.join(header.split())
        header = '_'.join(header.split('('))
        header = 'pct'.join(header.split('%'))
        header = 'dollar'.join(header.split('$'))
        header = header.strip('?)')
        if header == 'capital_gain': return 'reinvest_capital_gain'
        elif header == 'most_recent_price': return 'price'
        elif header == 'most_recent_change': return 'change'
        elif header == 'most_recent_value': return 'market_value'
        elif header == 'change_since_last_close_dollar': return 'day_change_dollar'
        elif header == 'change_since_last_close_pct': return 'day_change_pct'
        elif header == 'description': return 'name'
        else: return header

    def fix_data(self, data):
        data = ''.join(data.split(',')).strip()
        data = ''.join(data.split('$'))
        data = ''.join(data.split('%'))
        if data == '--': return None
        elif data.lower() == 'n/a': return None
        elif data == '': return None
        elif data == 'Cash & Money Market': return 'Cash'
        else: return data

    def get_id(self, acct_key):
        db_acc = self.session.query(Account).filter_by(**acct_key).first()
        if not db_acc:
            db_acc = Account(**acct_key)
            self.session.add(db_acc)
            self.session.commit()
        return db_acc.id

    def init_logger(self, logfilename):
        hdlr = TimedRotatingFileHandler(logfilename, when='midnight')
        fmt = logging.Formatter(fmt=self.logger_format)
        hdlr.setFormatter(fmt)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.INFO)

    def init_db_connection(self, dbname, dbhost):
        self.logger.info('Connecting to db %s...' % dbname)
        dburl = 'postgresql+psycopg2://%s/%s' % (dbhost, dbname)
        self.engine = create_engine(dburl)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.logger.info('Connected to db %s' % dbname)

    def write_rows(self, acct_id, base, data_rows):
        for data_row in data_rows:
            try:
                self.session.add(Position(**dict(data_row.items() + base)))
                self.session.commit()
            except IntegrityError as err:
                if 'duplicate key' in str(err):
                    msg = "Already have position data for account id %3i" 
                    msg += " for symbol %6s at %s %s"
                    msg = msg % (acct_id, data_row['symbol'], 
                                    self.date, self.time)
                    self.logger.info(msg)
                    pass
                else: raise err
                self.session.rollback()        

        
class Schwab2DB(AddDBMixin):
#    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
#                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s: %(message)s')
    logger = logging.getLogger('Schwab2DB')
    institution = ('institution', 'Schwab')
    fmt='%m/%d/%Y %H:%M:%S'

    def __init__(self, dbname, dbhost=''):
        self.init_logger('schwab2db.log')
        self.init_db_connection(dbname, dbhost)

    def process_position_file(self, filename):
        self.logger.info('Processing %s' % filename)
        start = datetime.now()

        # Get the timestamp
        pos_file = open(filename, 'r')
        lines = [x.strip() for x in pos_file.read().split('\n')]
        pos_line = [x for x in lines if 'positions' in x.lower()][0]
        dateinfo = pos_line.split('as of ')[-1].split()[0:2]
        self.date, self.time = self.add_timezone(*dateinfo, fmt=self.fmt)

        # Split up the data by account
        self.accts = [x for x in lines if 'xxxx' in x.lower()]
        totals = [x for x in lines if 'total market value' in x.lower()]
        starts = [lines.index(x) for x in self.accts]
        ends = [lines[x:].index(y) + x for x,y in zip(starts, totals)]
        ranges = zip(starts, ends)
        data = ['\n'.join(lines[x[0]+1:x[1]]) for x in ranges]
        data = [csv.DictReader(StringIO(x)) for x in data]
        self.data = [[dict([(self.fix_header(z[0]), self.fix_data(z[1])) 
                      for z in y.items() if self.fix_data(z[1])]) for y in x]
                     for x in data]

        end = datetime.now()
        self.logger.info('Processing completed. Took %0.3f seconds'
                         % self.seconds_elapsed(start, end))
        self.add_to_db()                         

    def add_to_db(self):
        self.logger.info('Adding positions to database')
        start = datetime.now()

        acct_keys = [dict([self.institution, ('account', x)]) 
                     for x in self.accts]
        acct_ids = [self.get_id(x) for x in acct_keys]
        for acct_id, acct_data in zip(acct_ids, self.data):
            base = [('id', acct_id), ('date', self.date), ('time', self.time)]
            self.write_rows(acct_id, base, acct_data)

        end = datetime.now()
        self.logger.info('Adding completed. Took %0.3f seconds'
                         % self.seconds_elapsed(start, end))

class Fidelity2DB(AddDBMixin):
#    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
#                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s: %(message)s')
    logger = logging.getLogger('Fidelity2DB')
    institution = ('institution', 'Fidelity')
    fmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self, dbname, dbhost=''):
        self.init_logger('fidelity2db.log')
        self.init_db_connection(dbname, dbhost)

    def process_position_file(self, filename):
        self.logger.info('Processing %s' % filename)
        start = datetime.now()

        # Get the timestamp from last file mod time
        modtime = time.localtime(os.path.getmtime(filename))
        modtime = time.strftime(self.fmt, modtime).split()
        self.date, self.time = self.add_timezone(*modtime, fmt=self.fmt)

        # Get the relevant data
        data = csv.DictReader(open(filename, 'r'))
        data = [dict([(y[0], y[1]) for y in x.items() if y[0]])  for x in data]
        data = [dict([(self.fix_header(y[0]), self.fix_data(y[1])) 
                      for y in x.items() if self.fix_data(y[1])])
                for x in data]
        self.acct_num = data[0]['account_name/number']
        self.data = [dict([(y[0], y[1]) for y in x.items() \
                     if y[0] != 'account_name/number'])  for x in data]

        end = datetime.now()
        self.logger.info('Processing completed. Took %0.3f seconds'
                         % self.seconds_elapsed(start, end))
        self.add_to_db()                         


    def add_to_db(self):
        self.logger.info('Adding positions to database')
        start = datetime.now()

        # Get account ids
        acct_key = dict([self.institution, ('account', self.acct_num)])
        acct_id = self.get_id(acct_key)
        base = [('id', acct_id), ('date', self.date), ('time', self.time)]
        self.write_rows(acct_id, base, self.data)

        end = datetime.now()
        self.logger.info('Adding completed. Took %0.3f seconds'
                         % self.seconds_elapsed(start, end))
            
# For running from command line
if __name__ == "__main__":
    pos_fname = sys.argv[1]
    db_name = sys.argv[2]
#    s2db = Schwab2DB(db_name)
#    s2db.process_position_file(pos_fname)
#    f2db = Fidelity2DB(db_name)
#    f2db.process_position_file(pos_fname)
