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
    butterfly_tickers = []
    symbol = ''
    ask_histogram = None
    bid_histogram = None
    
    def tickPrice(self, tickerId, field, price, canAutoExecute):
        msg = EWrapperMsgGenerator.tickPrice(tickerId, field, price, 
                                             canAutoExecute)
        if tickerId in self.butterfly_tickers:
            dt = datetime.now().isoformat()
            index = self.butterfly_tickers.index(tickerId)
            if field == 1:
                self.bid_histogram.datafile = '%s_BID_%s.dat' % (self.symbol, dt)
                self.bid_histogram.update_price(price, index)
                self.bid_histogram.plot_histogram(fname='%s_BID_%s.jpg'\
                                                       % (self.symbol, dt), dt)
            elif field == 2:
                self.ask_histogram.datafile = '%s_ASK_%s.dat' % (self.symbol, dt)
                self.ask_histogram.update_price(price, index)
                self.ask_histogram.plot_histogram(fname='%s_ASK_%s.jpg'\
                                                       % (self.symbol, dt), dt)
        self.datahandler(tickerId, msg)

if __name__ == "__main__":
    symbol = sys.argv[1]
    expiry = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]

    strikes = range(int(start), int(end)+1)
    strike_intervals = zip(strikes[:-1], strikes[1:])

    c = ButterClient()
    c.connect()
    c.ask_histogram = ButterflyHistogram(strike_intervals)
    c.bid_histogram = ButterflyHistogram(strike_intervals)
    c.symbol = symbol.upper()
    butterfly_strikes = zip(strikes[:-2], strikes[1:-1], strikes[2:])
    butterfly_conkeys = [(Option(symbol, expiry, 'C', x[0]),
                          Option(symbol, expiry, 'C', x[1]),
                          Option(symbol, expiry, 'C', x[2]))
                         for x in butterfly_strikes]
    butterfly_conids = [(c.request_contract_details(x[0])[0].m_summary.m_conId,
                         c.request_contract_details(x[1])[0].m_summary.m_conId,
                         c.request_contract_details(x[2])[0].m_summary.m_conId)
                        for x in butterfly_conkeys]
    butterflies = [Butterfly(*x) for x in butterfly_conids]
    c.butterfly_tickers = [c.request_mkt_data(x.contract, snapshot=False, 
                            fname='%s.mkt' % x.conId) for x in butterflies]

    signal.signal(signal.SIGINT, cleanup)
