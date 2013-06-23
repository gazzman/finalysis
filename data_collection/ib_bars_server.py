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

from finalysis.bar_orms import gen_table

LOGLEVEL = logging.INFO
PKEY = ['symbol', 'timestamp']


class ForkedTCPServer(SocketServer.ForkingMixIn, SocketServer.TCPServer):
    pass

class ForkedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # Parse message
        message = self.request.recv(1024).strip()
        logger.debug('Message received')
        data = [x.strip() for x in message.split(',')]
        db_name, schema, tablename = data[0:3]
        row = dict([x.split('=') for x in data[3:] if 'None' not in x])

        # Check to make sure we have primary key data
        for k in PKEY:
            try:
                assert k in row
            except AssertionError:
                logger.error('%s not in row data', k)
                return False

        # Add timezone info to the timestamp
        try:
            date, time = row['timestamp'].split()
            row['timestamp'] = add_timezone(date, time)
        except ValueError:
            return False

        # Extract the primary key
        key = ['%s=%s' % item for item in row.items() if k in PKEY]
        logger.debug('Data is %s', row)

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
        table = gen_table(tablename, metadata, schema=schema)
        try: metadata.create_all()
        except (ProgrammingError, IntegrityError) as err: logger.error(err)

        # Insert or update row in table
        try:
            conn.execute(table.insert(), **row)
            logger.debug('Inserted %s', row)
        except IntegrityError as err:
            if 'duplicate key' in str(err):
                data = dict([(k, v) for k, v in row.items() if k not in PKEY])
                upd = table.update(values=data)\
                           .where(table.c.underlying==row['symbol'])\
                           .where(table.c.timestamp==row['timestamp'])
                conn.execute(upd)
                logger.info('Updated %s with %s', key, data)
            else: raise(err)
        conn.close()
        logger.debug('Closed connection to db %s', db_name)
        logger.info('Wrote data in %s.%s for %s', schema, tablename, key)

def add_timezone(date, time, locale='US/Eastern', fmt='%Y%m%d %H:%M:%S'):
    tz = timezone(locale)
    dt = ' '.join([date, time])
    dt = datetime.strptime(dt, fmt)
    tzone = tz.tzname(dt)
    return ' '.join([dt.date().isoformat(), dt.time().isoformat(), tzone])

def cleanup(signal, frame):
    server.server_close()
    logger.warn('BAR2DB server shutdown')
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
    logger.warn('BAR2DB server started. Listeing on socket %s:%i', HOST, PORT)
    logger.info('Format messages as "db_name, schema, tablename, bar"')
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    server.serve_forever()
