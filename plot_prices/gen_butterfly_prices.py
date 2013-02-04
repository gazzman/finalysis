#!/usr/local/bin/jython
from datetime import datetime
import logging
import os
import signal
import sys

from com.ib.client import EWrapperMsgGenerator

from ib.client import Client
from ib.contractkeys import Option, Stock
from ib.combo_orders import Butterfly
from finalysis.plot_prices.butterfly_prices import (ButterflyPrices,
                                                    gen_strike_intervals)

LOGLEVEL = logging.DEBUG

def cleanup(signal, frame):
	c.disconnect()
	sys.exit(0)

class ButterPriceClient(Client):
    prices = dict()
    tickerId_to_symbolKey = dict()

    def tickPrice(self, tickerId, field, price, canAutoExecute):
        msg = EWrapperMsgGenerator.tickPrice(tickerId, field, price, 
                                             canAutoExecute)
        dt = datetime.now().isoformat()
        try:
            key, index = self.tickerId_to_symbolKey[tickerId]
            prices = self.prices[key]
            symExpRight = '_'.join(key)
            if field in [1, 2]:
                prices.ofile = '%s/butterflies_%s.dat' % (symExpRight, dt)
                prices.ufile = '%s/underlying_%s.dat' % (symExpRight, dt)
                prices.update_price(price, index, field)
                prices.plot_prices(fname='%s/%s.jpg'\
                                             % (symExpRight, dt), timestamp=dt)
        except KeyError:
            pass
        self.datahandler(tickerId, msg)

if __name__ == "__main__":
    ''' format of the symbols_file is:

        ticker_symbol expiry_date starting_strike ending_strike right increment

    '''
    symbols_file = sys.argv[1]

    c = ButterPriceClient()
    c.connect()

    f = open(symbols_file, 'r')
    for line in f:
        symbol, expiry, start, end, right, increment = line.split()
        key = (symbol, expiry, right)
        if not os.path.exists('_'.join(key)): os.makedirs('_'.join(key))

        strikes, strike_intervals = gen_strike_intervals(start, end, increment)
        if not strike_intervals: raise Exception('Strike interval error')
        
        c.prices[key] = ButterflyPrices(strike_intervals)
        butterfly_strikes = zip(strikes[:-2], strikes[1:-1], strikes[2:])
        butterfly_conkeys = [(Option(symbol, expiry, right, x[0]),
                              Option(symbol, expiry, right, x[1]),
                              Option(symbol, expiry, right, x[2]))
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
        stock = Stock(symbol)                  
        stock_contract = c.request_contract_details(stock)[0].m_summary
        tickerIds.append(c.request_mkt_data(stock_contract, snapshot=False,
                         fname='%s.mkt' % symbol))

        for tickerId in tickerIds:
            index = tickerIds.index(tickerId)
            c.tickerId_to_symbolKey[tickerId] = (key, index)

    signal.signal(signal.SIGINT, cleanup)
