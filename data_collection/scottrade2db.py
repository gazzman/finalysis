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

from finalysis.account_orms import (Account, Base, Position, 
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

    pos_data['id'] = get_id(account_info, session)

    # Get the timestamp
    f = open(pos_fname, 'r')
    pos_data['timestamp'] = f.readline().strip().split()[-1]

    c = csv.DictReader(f)
    for row in c:
        pos_data.update(gen_position_data(row, scottrade_map))
        session.add(Position(**pos_data))
        try: session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err): session.rollback()
            else: raise(err)
