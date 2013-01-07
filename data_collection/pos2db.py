#!/usr/bin/python
from sqlalchemy import Column, ForeignKey, Integer, String, Time
from sqlalchemy.orm import backref, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import (CHAR, DATE, INTEGER, 
                                            NUMERIC, VARCHAR)

Base = declarative_base()

class Account(Base):
    __tablename__ = 'accounts'
    # Identifying fields    
    id = Column(Integer, primary_key=True, unique=True)
    institution = Column(String, primary_key=True)
    account = Column(String, primary_key=True)

    positions = relationship('Position', backref='accounts')

class Position(Base):
    __tablename__ = 'positions'
    id = Column(Integer, ForeignKey('accounts.id'), primary_key=True,
                index=True)
    date = Column(DATE, primary_key=True, index=True)
    time = Column(Time(timezone=True), primary_key=True, index=True)

    # Schwab and Fidelity fields
    symbol = Column(VARCHAR(6), primary_key=True, index=True)
    name = Column(String)
    quantity = Column(NUMERIC(17,4))
    price = Column(NUMERIC(16,3))
    change = Column(NUMERIC(16,3))
    market_value = Column(NUMERIC(16,3))
    day_change_dollar = Column(NUMERIC(16,3))
    day_change_pct = Column(NUMERIC(9,2))
    reinvest_dividends = Column(VARCHAR(3))
    reinvest_capital_gain = Column(VARCHAR(3))
    pct_of_account = Column(NUMERIC(9,2))
    security_type = Column(String)
    
class AddDBMixin():
    def add_timezone(self, date, time, fmt, locale='US/Eastern'):
        tz = timezone(locale)
        dt = ' '.join([date, time])
        dt = datetime.strptime(dt, fmt)
        tzone = tz.tzname(dt)
        return dt.date().isoformat(), ' '.join([dt.time().isoformat(), tzone])

    def seconds_elapsed(self, start, end):
        s = (end - start).seconds
        m = (end - start).microseconds
        return round(s + m/1000000.0,3)


    def fix_header(self, header):
        header = header.lower().strip()
        header = '_'.join(header.split())
        header = '_'.join(header.split('('))
        header = 'pct'.join(header.split('%'))
        header = 'dollar'.join(header.split('$'))
        header = header.strip('?)')
        if header == 'capital_gain': return 'reinvest_capital_gain'
        elif header == 'most_recent_price': return 'price'
        elif header == 'most_recent_change': return 'change'
        elif header == 'most_recent_value': return 'market_value'
        elif header == 'change_since_last_close_dollar': return 'day_change_dollar'
        elif header == 'change_since_last_close_pct': return 'day_change_pct'
        elif header == 'description': return 'name'
        else: return header


    def fix_data(self, data):
        data = ''.join(data.split(',')).strip()
        data = ''.join(data.split('$'))
        data = ''.join(data.split('%'))
        if data == '--': return None
        elif data.lower() == 'n/a': return None
        elif data == '': return None
        elif data == 'Cash & Money Market': return 'Cash'
        else: return data


    def get_id(self, account, session):
        db_acc = session.query(Account).filter_by(**account).first()
        if not db_acc:
            db_acc = Account(**account)
            session.add(db_acc)
            session.commit()
        return db_acc.id
        
class Schwab2DB(AddDBMixin):
    pos_fname = sys.argv[1]
    db_name = sys.argv[2]
    institution = ('institution', 'Schwab')
    pos_time = {}

    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger = logging.getLogger('schwab2db')
    hdlr = TimedRotatingFileHandler('schwab2db.log', when='midnight')
    fmt = logging.Formatter(fmt=logger_format)
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    logger.info('Reading ' + pos_fname)
    start = datetime.now()

    # Get the timestamp
    pos_file = open(pos_fname, 'r')
    lines = [x.strip() for x in pos_file.read().split('\n')]
    pos_line = [x for x in lines if 'positions' in x.lower()][0]
    dateinfo = pos_line.split('as of ')[-1].split()[0:2]
    pos_time['date'], pos_time['time'] = add_timezone(*dateinfo)

    # Split up the data by account
    accounts = [x for x in lines if 'xxxx' in x.lower()]
    totals = [x for x in lines if 'total market value' in x.lower()]

    starts = [lines.index(x) for x in accounts]
    ends = [lines[x:].index(y) + x for x,y in zip(starts, totals)]
    ranges = zip(starts, ends)

    data = ['\n'.join(lines[x[0]+1:x[1]]) for x in ranges]
    data = [csv.DictReader(StringIO(x)) for x in data]

    data = [[dict([(fix_header(z[0]), fix_data(z[1])) for z in y.items() \
            if fix_data(z[1])]) for y in x] for x in data]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get account ids
    accounts = [dict([institution, ('account', x)]) for x in accounts]
    account_ids = [get_id(x, session) for x in accounts]

    for a, d in zip(account_ids, data):
        for row in d:
            try:
                session.add(Position(**dict(row.items() + pos_time.items() 
                                            + [('id', a)])))
                session.commit()
            except IntegrityError as err:
                if 'duplicate key' in str(err):
                    msg = 'Already have position data for account id ' + str(a)
                    msg += ' for ticker ' + row['symbol'] 
                    msg += ' at ' + ' '.join(dateinfo)
                    logger.info(msg)
                    pass
                else: raise err
                session.rollback()        

class Fidelity2DB(AddDBMixin):
    pos_fname = sys.argv[1]
    db_name = sys.argv[2]
    institution = ('institution', 'Fidelity')
    pos_time = {}

    logger_format = ('%(levelno)s, [%(asctime)s #%(process)d]'
                     + '%(levelname)6s -- %(threadName)s: %(message)s')
    logger = logging.getLogger('fidelity2db')
    hdlr = TimedRotatingFileHandler('fidelity2db.log', when='midnight')
    fmt = logging.Formatter(fmt=logger_format)
    hdlr.setFormatter(fmt)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    logger.info('Reading ' + pos_fname)
    start = datetime.now()

    # Get the timestamp from last file mod time
    modtime = time.localtime(os.path.getmtime(pos_fname))
    modtime = time.strftime('%m/%d/%Y %H:%M:%S', modtime).split()
    pos_time['date'], pos_time['time'] = add_timezone(*modtime)

    data = csv.DictReader(open(pos_fname, 'r'))
    data = [dict([(y[0], y[1]) for y in x.items() if y[0]])  for x in data]
    data = [dict([(fix_header(y[0]), fix_data(y[1])) 
                   for y in x.items() if fix_data(y[1])])  for x in data]
    account_number = data[0]['account_name/number']

    data = [dict([(y[0], y[1]) for y in x.items() \
                  if y[0] != 'account_name/number'])  for x in data]

    # Connect to db
    dburl = 'postgresql+psycopg2:///' + db_name
    engine = create_engine(dburl)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get account ids
    account = dict([institution, ('account', account_number)])
    account_id = get_id(account, session)

    for row in data:
        try:
            session.add(Position(**dict(row.items() + pos_time.items() 
                                        + [('id', account_id)])))
            session.commit()
        except IntegrityError as err:
            if 'duplicate key' in str(err):
                msg = 'Already have position data for account id '
                msg += str(account_id) + ' for ticker ' + row['symbol'] 
                msg += ' at ' + ' '.join(modtime)
                logger.info(msg)
                pass
            else: raise err
            session.rollback()
