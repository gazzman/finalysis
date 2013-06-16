#!/usr/bin/python
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
from datetime import datetime
import SocketServer
import logging
import signal
import sys
import threading

from pytz import timezone
from sqlalchemy import create_engine, MetaData
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import IntegrityError, ProgrammingError

from finalysis.option_chain_orms import *

LOGLEVEL = logging.INFO

class ForkedTCPServer(SocketServer.ForkingMixIn, SocketServer.TCPServer):
    pass

class ForkedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # Parse message
        logger.debug('Message received')
        message = self.request.recv(1024).strip()
        data = [x.strip() for x in message.split(',')]
        db_name, schema, tablename, links, right = data[0:5]
        row = dict([x.split('=') for x in data[5:] if 'None' not in x])
        for k in ['underlying', 'strike_interval', 'timestamp']:
            try:
                assert k in row
            except AssertionError:
                logger.error('%s not in row data', k)
                return False
                
        row['timestamp'] = add_timezone(*row['timestamp'].split())
        
        # Connect to db
        logger.debug('Connecting to db %s', db_name)
        dburl = 'postgresql+psycopg2:///%s' % (db_name)
        engine = create_engine(dburl)
        conn = engine.connect()
        logger.debug('Connected to db %s', db_name)

        # Create schema and table if necessary
        try: engine.execute(CreateSchema(schema))
        except ProgrammingError: pass
        metadata = MetaData(engine)
        table = gen_table(tablename, metadata, links=links, right=right, 
                          schema=schema)
        metadata.create_all()

        # Insert row into table
        try:
            conn.execute(table.insert(), **row)
            logger.debug('Inserted db with %s' % message)
        except IntegrityError as err:
            if 'duplicate key' in str(err):
                upd = table.update()\
                           .where(table.c.underlying==row['underlying'])\
                           .where(table.c.strike_interval==row['strike_interval'])\
                           .where(table.c.timestamp==row['timestamp'])
                conn.execute(upd, **row)
                logger.debug('Updated db with %s' % message)
            else: raise(err)
        conn.close()
        logger.info('Wrote data in %s.%s for %s at %s', schema, tablename, 
                                                        row['underlying'],
                                                        row['timestamp'])

def add_timezone(date, time, locale='US/Eastern', fmt='%Y%m%d %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return ' '.join([dt.date().isoformat(), dt.time().isoformat(), tzone])

def cleanup(signal, frame):
    server.server_close()
    logger.warn('CHAIN2DB server shutdown')
    sys.exit(0)

if __name__ == '__main__':
    HOST = sys.argv[1]
    PORT = int(sys.argv[2])

    # Initialize logging
    logger_fmt = ' '.join(['%(levelno)s, [%(asctime)s #%(process)5i]',
                           '%(levelname)8s: %(message)s'])
    logger = logging.getLogger(__name__)
    hdlr = logging.StreamHandler()
    fmt = logging.Formatter(fmt=logger_fmt)
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(LOGLEVEL)

    # Start the server
    server = ForkedTCPServer((HOST, PORT), ForkedTCPRequestHandler)
    logger.warn('CHAIN2DB server started. Listeing on socket %s:%i', HOST, PORT)
    mfmt = 'db_name, schema, tablename, links, right, data'
    logger.info('Format messages as %s', mfmt)
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    server.serve_forever()
