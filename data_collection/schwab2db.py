#!/usr/bin/python
from datetime import datetime
from StringIO import StringIO
import csv
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base

from finalysis.data_collection.account_orms import (Account, Base, Position, 
                                                    add_timezone, get_id,
                                                    gen_position_data, SCHEMA)
from finalysis.data_collection.fieldmaps import schwab_map

# For running from command line
if __name__ == "__main__":
    pos_fname = sys.argv[1]
    db_name = sys.argv[2]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)
    try: engine.execute(CreateSchema(SCHEMA))
    except ProgrammingError: pass

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    account_info = {'institution': 'Schwab'}
    pos_data = {}

    # Get the timestamp
    pos_file = open(pos_fname, 'r')
    lines = [x.strip() for x in pos_file.read().split('\n')]
    pos_line = [x for x in lines if 'positions' in x.lower()][0]
    dateinfo = pos_line.split('as of ')[-1].split()[0:2]
    pos_data['date'], pos_data['time'] = add_timezone(*dateinfo)

    # Split up the data by account
    accounts = [x for x in lines if 'xxxx-' in x.lower()]
    totals = [x for x in lines if 'total market value' in x.lower()]

    starts = [lines.index(x) for x in accounts]
    ends = [lines[x:].index(y) + x for x,y in zip(starts, totals)]
    ranges = zip(starts, ends)

    data = ['\n'.join(lines[x[0]+1:x[1]]) for x in ranges]
    data = [csv.DictReader(StringIO(x)) for x in data]
    for i in range(0, len(accounts)):
        account_info.update({'account': accounts[i]})
        pos_data['id'] = get_id(account_info, session)
        for row in data[i]:
            pos_data.update(gen_position_data(row, schwab_map,
                                              cashdesc='Brokerage'))
            session.add(Position(**pos_data))
            try: session.commit()
            except IntegrityError as err:
                if 'duplicate key' in str(err): session.rollback()
                else: raise(err)
