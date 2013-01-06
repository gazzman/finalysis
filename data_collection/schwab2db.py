#!/usr/bin/python
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from StringIO import StringIO
import csv
import logging
import sys

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
    header = header.lower()
    header = '_'.join(header.split())
    header = '_'.join(header.split('('))
    header = 'pct'.join(header.split('%'))
    header = 'dollar'.join(header.split('$'))
    header = header.strip('?)')
    if header == 'capital_gain': header = 'reinvest_capital_gain'
    return header


def fix_data(data):
    data = ''.join(data.split(','))
    data = ''.join(data.split('$'))
    data = ''.join(data.split('%'))
    if data == '--': return None
    elif data == 'N/A': return None
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
    institution = ('institution', 'Schwab')
    pos_time = {}

    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger = logging.getLogger('schwab2db')
    hdlr = TimedRotatingFileHandler('schwab2db.log', when='midnight')
    fmt = logging.Formatter(fmt=logger_format)
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    logger.info('Reading ' + pos_fname)
    start = datetime.now()

    # Get the timestamp
    pos_file = open(pos_fname, 'r')
    lines = [x.strip() for x in pos_file.read().split('\n')]
    pos_line = [x for x in lines if 'positions' in x.lower()][0]
    dateinfo = pos_line.split('as of ')[-1].split()[0:2]
    pos_time['date'], pos_time['time'] = add_timezone(*dateinfo)

    # Split up the data by account
    accounts = [x for x in lines if 'xxxx' in x.lower()]
    totals = [x for x in lines if 'total market value' in x.lower()]

    starts = [lines.index(x) for x in accounts]
    ends = [lines[x:].index(y) + x for x,y in zip(starts, totals)]
    ranges = zip(starts, ends)

    data = ['\n'.join(lines[x[0]+1:x[1]]) for x in ranges]
    data = [csv.DictReader(StringIO(x)) for x in data]

    data = [[dict([(fix_header(z[0]), fix_data(z[1])) for z in y.items() \
            if fix_data(z[1])]) for y in x] for x in data]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get account ids
    accounts = [dict([institution, ('account', x)]) for x in accounts]
    account_ids = [get_id(x, session) for x in accounts]

    for a, d in zip(account_ids, data):
        for row in d:
            try:
                session.add(Position(**dict(row.items() + pos_time.items() 
                                            + [('id', a)])))
                session.commit()
            except IntegrityError as err:
                if 'duplicate key' in str(err):
                    msg = 'Already have position data for account id ' + str(a)
                    msg += ' for ticker ' + row['symbol'] 
                    msg += ' at ' + ' '.join(dateinfo)
                    logger.info(msg)
                    pass
                else: raise err
                session.rollback()
