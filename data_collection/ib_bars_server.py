#!/usr/bin/python
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
import SocketServer
import logging
import signal
import threading

from finalysis.data_collection.ibbars2db import *

LOGLEVEL = logging.INFO

def cleanup(signal, frame):
    server.server_close()
    logger.warn('BAR2DB server shutdown')
    sys.exit(0)

class ForkedTCPServer(SocketServer.ForkingMixIn, SocketServer.TCPServer):
    pass

class ForkedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        # Parse message
        logger.debug('Message received')
        message = self.request.recv(1024).strip()
        data = [x.strip() for x in message.split(',')]
        db_name, schema, fname_list, barline = data[0], data[1], data[2:3], data[3]
        datafile, tablename, symbol = parse_fname_list(fname_list)

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
        metadata.create_all()

        timestamp, bar = parse_barline(barline.lower())
        bar_to_db(conn, table, symbol, timestamp, bar)
        conn.close()
        logger.info('Wrote bar in %s.%s for %s at %s', schema, tablename, 
                                                          symbol, timestamp)

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
    logger.info('Format messages as "db_name, schema, fname_list, barline"')
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    server.serve_forever()
