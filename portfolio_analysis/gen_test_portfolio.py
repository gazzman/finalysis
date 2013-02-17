#!/usr/bin/python
import argparse
import csv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema

from finalysis.account_orms import (Account, Position, get_id, 
                                    gen_position_data)

fieldmap = {'symbol': 'symbol',
            'description': 'description',
            'qty': 'qty',
            'price': 'price',
            'total_value': 'total_value'}

# For running from command line
if __name__ == "__main__":
    def_schema='test_portfolio'
    description = 'A script for storing a csv portfolio in a database.'
    p = argparse.ArgumentParser(description=description)
    p.add_argument('pos_fname', type=str, help='name of the csv file')
    p.add_argument('db_name', type=str, help='name of the postgresql database')
    p.add_argument('--schema', default=def_schema,
                   help="positions table schema; default is '%s'" % def_schema)
    args = p.parse_args()
    pos_fname = args.pos_fname
    db_name = args.db_name
    schema = args.schema

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    # Create schema and tables
    try: engine.execute(CreateSchema(schema))
    except ProgrammingError: pass
    Account.__table__.schema = schema
    Position.__table__.schema = schema
    Account.__table__.create(engine, checkfirst=True)
    Position.__table__.create(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create the account entry
    account_info = {'institution': 'Test', 'account': 'Test'}
    pos_data = {}
    pos_data['id'] = get_id(account_info, session)

    # Get the timestamp
    f = open(pos_fname, 'r')
    pos_data['timestamp'] = f.readline().strip().split()[-1]

    c = csv.DictReader(f)
    for row in c:
        pos_data.update(gen_position_data(row, fieldmap))
        session.add(Position(**pos_data))
        try: session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err): session.rollback()
            else: raise(err)
