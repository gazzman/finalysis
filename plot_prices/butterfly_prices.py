#!/usr/bin/python
import sys

from Gnuplot import Gnuplot

from finalysis.combo_gnuplots import GNUPlotBase

def gen_strike_intervals(start, end, increment):
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
    else: return None
    strike_intervals = zip(strikes[:-1], strikes[1:])
    return strike_intervals

class ButterflyPrices():
    price_codes = [1, 2]
    ohead = '#strike bid ask mid\n'
    oline = '%0.2f %0.3f %0.3f %0.3f\n'
    uhead = '#price 0 color\n'
    uline = '%0.3f 0 2\n%0.3f 0 1\n%0.3f 0 0\n'
    plot = 'plot "%(ofile)s" using 1:2 with boxes lc 2' # boxplots of bids
    plot +=   ', "%(ofile)s" using 1:3 with boxes lc 1' # boxplots of asks
    plot +=   ', "%(ofile)s" using 1:4 with boxes lc 0' # boxplots of midpoint
    plot +=   ', "%(ufile)s" ps 2 pt 7 lc variable'     # underlying prices
    plotkey = 'unset parametric\nset key on\n'
    plotkey += 'plot [][0:1] 2 lc 2 title "BID"' # BID key
    plotkey +=            ', 2 lc 1 title "ASK"' # ASK key
    plotkey +=            ', 2 lc 0 title "MID"' # MID key

    def __init__(self, strike_intervals):
        max_payoff = 2*max([x[1] - x[0] for x in strike_intervals])
        self.intervals = [x for x in strike_intervals]
        self.prices = [[0, 0, 0] for x in self.intervals]
        self.gpbase = GNUPlotBase()
        self.gpbase.xmin = strike_intervals[0][0]*.9
        self.gpbase.xmax = strike_intervals[-2][-1]*1.1
        self.gpbase.ymin = -1*max_payoff
        self.gpbase.ymax = max_payoff
        self.ofile = 'butterprices.dat'
        self.ufile = 'underprices.dat'
        self.xticks = [x[0] for x in strike_intervals 
                                         if strike_intervals.index(x) % 2 == 0]
        self.xticks.append(strike_intervals[-1][-1])
        self.yticks = [max_payoff/5.0*x for x in range(-5, 6)]
        self.intervals[-1] = ('Spot',)
        self.g = Gnuplot()

    def mid(self, interval):
        return 0.5*(interval[1] + interval[0])

    def update_price(self, price, index, code):
        self.prices[index][self.price_codes.index(code)] = price
        self.prices[index][-1] = self.mid(self.prices[index][:2])
        f1 = open(self.ofile, 'w')
        f1.write(self.ohead)
        for i in range(0, len(self.prices)-1):
            f1.write(self.oline 
                          % ((self.intervals[i][-1],) + tuple(self.prices[i])))
        f1.close()
        f2 = open(self.ufile, 'w')
        f2.write(self.uhead)
        f2.write(self.uline % tuple(self.prices[-1]))
        f2.close()

    def plot_prices(self, fname=None, timestamp=None):
        commands = []
        commands.append(self.gpbase.set_output(fname=fname))
        commands.append(self.gpbase.gen_ticks(self.xticks, self.yticks))
        commands.append(self.gpbase.gen_header(xlabel='Spot Price at Expiry',
                               ylabel='Butterfly Price',
                               timestamp=timestamp))
        commands.append(self.plot % {'ofile': self.ofile, 'ufile': self.ufile})
        commands.append(self.plotkey)
        self.g('\n'.join(commands))
        return '\n'.join(commands)

if __name__ == '__main__':
    mktdatafname = sys.argv[1]
    start, end, increment = mktdatafname.split('.')[0].split('_')
    strike_intervals = gen_strike_intervals(start, end, increment)
    bp = ButterflyPrices(strike_intervals)
    with open(mkdatafname) as f:
        for line in f:
            index, dt, tickerId, data = line.split()[:4]
            field, price = data.split('=')
            if field == 'bidPrice': field = 1
            elif field == 'askPrice': field = 2
            if type(field) == int:
                bp.ofile = 'butterflies_%s.dat' % dt
                bp.ufile = 'underlying_%s.dat' % dt
                bp.update_price(float(price), int(index), field)
                bp.plot_prices(fname='%s.jpg' % dt, timestamp=dt)
