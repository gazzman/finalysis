#!/usr/bin/python
from finalysis.combo_gnuplots import GNUPlotBase

class ButterflyPrices():
    def __init__(self, strike_intervals):
        max_payoff = 2*max([x[1] - x[0] for x in strike_intervals])
        self.intervals = [x for x in strike_intervals]
        self.prices = [0 for x in self.intervals]
        self.gpbase = GNUPlotBase()
        self.gpbase.xmin = strike_intervals[0][0]*.9
        self.gpbase.xmax = strike_intervals[-2][-1]*1.1
        self.gpbase.ymin = -1*max_payoff
        self.gpbase.ymax = max_payoff
        self.datafile = 'butterprices.dat'
        self.line = '%0.2f %0.3f\n'
        self.xticks = [x[0] for x in strike_intervals 
                                         if strike_intervals.index(x) % 2 == 0]
        self.xticks.append(strike_intervals[-1][-1])
        self.yticks = [max_payoff/5.0*x for x in range(-5, 6)]
        self.intervals[-1] = ('Spot',)

    def update_price(self, price, index):
        self.prices[index] = price
        f = open(self.datafile, 'w')
        f.write('#Underlying %0.3f\n' % self.prices[-1])
        for i in range(0, len(self.prices)-1):
            f.write(self.line % (self.intervals[i][-1], self.prices[i]))
        f.close()
        

    def plot_prices(self, fname=None, timestamp=None):
        commands = []
        commands.append(self.gpbase.set_output(fname=fname))
        commands.append(self.gpbase.gen_ticks(self.xticks, self.yticks))
        commands.append(self.gpbase.gen_header(xlabel='Spot Price at Expiry',
                               ylabel='Butterfly Price',
                               timestamp=timestamp))
        commands.append('plot "%s" with boxes' % self.datafile)
        return '\n'.join(commands)
