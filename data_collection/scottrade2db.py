#!/usr/bin/python
from datetime import datetime
import csv
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base

from finalysis.data_collection.account_orms import (Account, Base, Position, 
                                                    add_timezone, get_id,
                                                    gen_position_data, SCHEMA)
from finalysis.data_collection.fieldmaps import scottrade_map

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
    pos_data['id'] = get_id(account_info, session)

    c = csv.DictReader(open(pos_fname, 'r'))
    for row in c:
        pos_data.update(gen_position_data(row, scottrade_map))
        session.add(Position(**pos_data))
        try: session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err): session.rollback()
            else: raise(err)
