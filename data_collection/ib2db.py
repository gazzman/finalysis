#!/usr/bin/python
from datetime import datetime
from decimal import Decimal
import sys
import xml.etree.ElementTree as etree

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base

from finalysis.account_orms import (Account, Base, Position, 
                                    add_timezone, get_id,
                                    gen_position_data, SCHEMA)

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

    account_info = {'institution': 'IB'}
    pos_data = {}

    tree = etree.parse(pos_fname)
    flexquery_response = tree.getroot()
    flexquery_statements = flexquery_response.getchildren()[0]
    for statement in flexquery_statements:
        account_info['account'] = statement.attrib['accountId']
        pos_data['id'] = get_id(account_info, session)
        date = statement.attrib['toDate']
        date, time = add_timezone(date, '235959', fmt='%Y%m%d %H%M%S')
        pos_data['timestamp'] = '%s %s' % (date, time)

        for report in statement:
            for position in report:
                if 'cash' in report.tag.lower():
                    cash = report.getchildren()[0].attrib['endingCash']
                    cash = Decimal('%16.3f' % float(cash))
                    pos_data['symbol'] = 'CASH'
                    pos_data['description'] = 'Brokerage'
                    pos_data['price'] = 1
                    pos_data['qty'] = cash
                    pos_data['total_value'] = cash
                else:                
                    pos_data['symbol'] = position.attrib['symbol']
                    pos_data['description'] = position.attrib['description']
                    pos_data['price'] = position.attrib['markPrice']
                    pos_data['qty'] = position.attrib['position']
                    pos_data['total_value'] = position.attrib['positionValue']
                session.add(Position(**pos_data))
                try: session.commit()
                except IntegrityError as err:
                    if 'duplicate key' in str(err): session.rollback()
                    else: raise(err)
