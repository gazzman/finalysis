#!/usr/bin/python
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
from datetime import datetime
#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
import argparse
import sys
import lxml.etree as etree

from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from finalysis.account_orms import (Account, Base, Transaction, 
                                    add_timezone, get_id)

XML_STYLESHEET = '<?xml-stylesheet type="text/xsl" href="/styles/daily_cash_by_underlying.xsl"?>'
PATH = '/FlexQueryResponse/FlexStatements/FlexStatement'
NULL_SYMBOLS = ['N/A', '--', '']

if __name__ == '__main__':
    description = 'A utility for storing IB Trades and Statements of Funds in a db'
    p = argparse.ArgumentParser(description=description)
    p.add_argument('xml_file', type=str, help='name of xml file produced by flex query')
    p.add_argument('db_host', type=str, help='db hostname')
    p.add_argument('db_name', type=str, help='db name')
    args = p.parse_args()

    # Connect to db
    dburl = 'postgresql+psycopg2://%s/%s' % (args.db_host, args.db_name)
    engine = create_engine(dburl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    with open(args.xml_file) as x:
        tree = etree.parse(x)
    trades = tree.xpath('%s/Trades/Trade' % PATH)
    trades_dict = dict([(t.get('tradeID'), t.attrib) for t in trades])
    sofls = tree.xpath('%s/StmtFunds/StatementOfFundsLine' % PATH)
    rows = []
    for sofl in sofls:
        tid = sofl.get('tradeID')
        if tid in trades_dict: 
            row = dict(sofl.attrib.items() + trades_dict[tid].items())
            try: row['commission'] = row.pop('ibCommission')
            except KeyError: pass
            try: row['commissionCurrency'] = row.pop('ibCommissionCurrency')
            except KeyError: pass
            row.pop('date')
            row['timestamp'] = ' '.join(add_timezone(row.pop('tradeDate'), 
                                                     row.pop('tradeTime'), 
                                                     fmt='%Y%m%d %H%M%S'))
        else: 
            row = dict(sofl.attrib)
            row['tradeID'] = -1
            row['timestamp'] = ' '.join(add_timezone(row.pop('date'), '160000',
                                                     fmt='%Y-%m-%d %H%M%S'))
        row['id'] = get_id({'account': row.pop('accountId'),
                            'institution': 'IB'}, session)
        row = dict([(k,row[k]) for k in row if row[k] not in NULL_SYMBOLS])
        t = Transaction(**row)
        tpk = dict([(k, row[k]) 
                    for k in t.__table__.primary_key.columns.keys()])
        session.add(t)
        try:
            session.commit()
            print >> sys.stderr, "Inserted", tpk
        except IntegrityError as err:
            session.rollback()
            if 'duplicate key value' in err.args[0]:
                print >> sys.stderr, err.args[0].split('\n')[1]
            else: raise err
    session.close()
