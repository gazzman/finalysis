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
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from positions_table import Account, Base, Position


def add_timezone(date, time, locale='US/Eastern', fmt='%m/%d/%Y %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return dt.date().isoformat(), ' '.join([dt.time().isoformat(), tzone])


def seconds_elapsed(start, end):
    s = (end - start).seconds
    m = (end - start).microseconds
    return round(s + m/1000000.0,3)


def fix_header(header):
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


def fix_data(data):
    data = ''.join(data.split(',')).strip()
    data = ''.join(data.split('$'))
    data = ''.join(data.split('%'))
    if data == '--': return None
    elif data.lower() == 'n/a': return None
    elif data == '': return None
    elif data == 'Cash & Money Market': return 'Cash'
    else: return data


def get_id(account, session):
    db_acc = session.query(Account).filter_by(**account).first()
    if not db_acc:
        db_acc = Account(**account)
        session.add(db_acc)
        session.commit()
    return db_acc.id

# For running from command line
if __name__ == "__main__":
    pos_fname = sys.argv[1]
    db_name = sys.argv[2]
    institution = ('institution', 'Fidelity')
    pos_time = {}

    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger = logging.getLogger('fidelity2db')
    hdlr = TimedRotatingFileHandler('fidelity2db.log', when='midnight')
    fmt = logging.Formatter(fmt=logger_format)
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    logger.info('Reading ' + pos_fname)
    start = datetime.now()

    # Get the timestamp from last file mod time
    modtime = time.localtime(os.path.getmtime(pos_fname))
    modtime = time.strftime('%m/%d/%Y %H:%M:%S', modtime).split()
    pos_time['date'], pos_time['time'] = add_timezone(*modtime)

    data = csv.DictReader(open(pos_fname, 'r'))
    data = [dict([(y[0], y[1]) for y in x.items() if y[0]])  for x in data]
    data = [dict([(fix_header(y[0]), fix_data(y[1])) 
                   for y in x.items() if fix_data(y[1])])  for x in data]
    account_number = data[0]['account_name/number']

    data = [dict([(y[0], y[1]) for y in x.items() \
                  if y[0] != 'account_name/number'])  for x in data]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get account ids
    account = dict([institution, ('account', account_number)])
    account_id = get_id(account, session)

    for row in data:
        try:
            session.add(Position(**dict(row.items() + pos_time.items() 
                                        + [('id', account_id)])))
            session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err):
                msg = 'Already have position data for account id '
                msg += str(account_id) + ' for ticker ' + row['symbol'] 
                msg += ' at ' + ' '.join(modtime)
                logger.info(msg)
                pass
            else: raise err
            session.rollback()
