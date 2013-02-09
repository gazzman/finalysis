#!/usr/bin/python
from datetime import datetime, timedelta
from StringIO import StringIO
import csv
import logging
import os
import sys

from pytz import timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base

from finalysis.data_collection.account_orms import (Account, Base, Position, 
                                                    gen_position_data, SCHEMA)
from finalysis.data_collection.fieldmaps import scottrade_map

def add_timezone(date, time, locale='US/Eastern', fmt='%m/%d/%Y %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return dt.date().isoformat(), ' '.join([dt.time().isoformat(), tzone])

def get_id(account_info):
    db_acc = session.query(Account).filter_by(**account_info).first()
    if not db_acc:
        db_acc = Account(**account_info)
        session.add(db_acc)
        session.commit()
    return db_acc.id

# For running from command line
if __name__ == "__main__":
    pos_fname = sys.argv[1]
    db_name = sys.argv[2]
    acct_num = sys.argv[3]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)
    try: engine.execute(CreateSchema(SCHEMA))
    except ProgrammingError: pass

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    account_info = {'institution': 'Scottrade', 'account': acct_num}
    pos_data = {}

    # Get the timestamp
    mtime = datetime.fromtimestamp(os.path.getmtime(pos_fname))
    date = mtime.date().strftime('%m/%d/%Y')
    time = mtime.time().strftime('%H:%M:%S')
    pos_data['date'], pos_data['time'] = add_timezone(date, time)
    pos_data['id'] = get_id(account_info)

    c = csv.DictReader(open(pos_fname, 'r'))
    for row in c:
        pos_data.update(gen_position_data(row, scottrade_map))
        session.add(Position(**pos_data))
        try: session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err): session.rollback()
            else: raise(err)
