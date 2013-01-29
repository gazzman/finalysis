#!/usr/local/bin/jython
from datetime import datetime
import logging
import signal
import sys

from com.ib.client import EWrapperMsgGenerator

from ib.client import Client
from ib.contractkeys import Option
from ib.combo_orders import Butterfly
from finalysis.density_estimation.butterfly_histogram import ButterflyHistogram

LOGLEVEL = logging.DEBUG

def cleanup(signal, frame):
	c.disconnect()
	sys.exit(0)

class ButterClient(Client):
    ask_histograms = dict()
    bid_histograms = dict()
    tickerId_to_symbolKey = dict()

    def tickPrice(self, tickerId, field, price, canAutoExecute):
        msg = EWrapperMsgGenerator.tickPrice(tickerId, field, price, 
                                             canAutoExecute)
        dt = datetime.now().isoformat()
        try:
            key, index = self.tickerId_to_symbolKey[tickerId]
            bid_histogram = self.bid_histograms[key]
            ask_histogram = self.ask_histograms[key]
            symExp = '_'.join(key)
            if field == 1:
                bid_histogram.datafile = '%s/BID_%s.dat' % (symExp, dt)
                bid_histogram.update_price(price, index)
                bid_histogram.plot_histogram(fname='%s/BID_%s.jpg'\
                                                  % (symExp, dt), timestamp=dt)
            elif field == 2:
                ask_histogram.datafile = '%s/ASK_%s.dat' % (symExp, dt)
                ask_histogram.update_price(price, index)
                ask_histogram.plot_histogram(fname='%s/ASK_%s.jpg'\
                                                  % (symExp, dt), timestamp=dt)
        except KeyError:
            pass
        self.datahandler(tickerId, msg)

if __name__ == "__main__":
    ''' format of the symbols_file is:

        ticker_symbol expiry_date starting_strike ending_strike increment

    '''
    symbols_file = sys.argv[1]

    c = ButterClient()
    c.connect()

    f = open(symbols_file, 'r')
    for line in f:
        symbol, expiry, start, end, increment = line.split()
        key = (symbol, expiry)
        if not os.path.exists('_'.join(key)): os.makedirs('_'.join(key))

        if float(increment) % 1 == 0:
            start = int(start)
            end = int(end)
            increment = int(increment)
            strikes = range(start, end+increment, increment)
        elif float(increment) == 0.5:
            start = int(float(start)*10.0)
            end = int(float(end)*10.0)
            increment = 5
            strikes = range(start, end+increment, increment)
            strikes = [x/10.0 for x in strikes]
        strike_intervals = zip(strikes[:-1], strikes[1:])

        c.ask_histograms[key] = ButterflyHistogram(strike_intervals)
        c.bid_histograms[key] = ButterflyHistogram(strike_intervals)
        butterfly_strikes = zip(strikes[:-2], strikes[1:-1], strikes[2:])
        butterfly_conkeys = [(Option(symbol, expiry, 'C', x[0]),
                              Option(symbol, expiry, 'C', x[1]),
                              Option(symbol, expiry, 'C', x[2]))
                                                    for x in butterfly_strikes]
        butterfly_conids = [(c.request_contract_details(x[0])[0]\
                              .m_summary.m_conId,
                             c.request_contract_details(x[1])[0]\
                              .m_summary.m_conId,
                             c.request_contract_details(x[2])[0]\
                              .m_summary.m_conId)
                                                    for x in butterfly_conkeys]
        butterflies = [Butterfly(*x) for x in butterfly_conids]
        tickerIds = [c.request_mkt_data(x.contract, snapshot=False, 
                                fname='%s.mkt' % x.conId) for x in butterflies]
        for tickerId in tickerIds:
            index = tickerIds.index(tickerId)
            c.tickerId_to_symbolKey[tickerId] = (key, index)

    signal.signal(signal.SIGINT, cleanup)
