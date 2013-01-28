#!/usr/bin/python
from finalysis.combo_gnuplots import GNUPlotBase

class ButterflyHistogram():
    gpbase = GNUPlotBase()
    datafile = 'butterhist.dat'
    intervals = list()
    prices = list()
    probs = list()
    price_sum = 0
    line = '%0.2f %0.3f\n'

    def __init__(self, strike_intervals):
        self.intervals = strike_intervals
        self.prices = [0 for x in self.intervals][:-1]
        self.probs = [1.0/len(self.intervals) for x in self.intervals]
        self.gpbase.xmin = strike_intervals[0][0]*.9
        self.gpbase.xmax = strike_intervals[-1][-1]*1.1
        self.gpbase.ymin = 0
        self.gpbase.ymax = 1
        self.xticks = [x[0] for x in strike_intervals]
        self.xticks.append(strike_intervals[-1][-1])
        self.yticks = [x/10.0 for x in range(1, 10)]

    def update_price(self, price, index):
        self.prices[index] = price
        self.calculate_probabilities()

    def sum_prices(self):
        self.price_sum = 0
        for x in self.prices: self.price_sum += x

    def calculate_probabilities(self):
        self.sum_prices()
        f = open(self.datafile, 'w')
        self.probs[0] = 0.5*self.prices[0]/self.price_sum
        f.write(self.line % (self.mid(self.intervals[0]), self.probs[0]))
        for i in range(1, len(self.probs)-1):
            self.probs[i] = self.mid(self.prices[i-1:i+1])/self.price_sum
            f.write(self.line % (self.mid(self.intervals[i]), self.probs[i]))
        self.probs[-1] = 0.5*self.prices[-1]/self.price_sum
        f.write(self.line % (self.mid(self.intervals[-1]), self.probs[-1]))
        f.close()

    def mid(self, interval):
        return (interval[1] + interval[0])/2.0

    def plot_histogram(self, fname=None):
        self.gpbase.set_output(fname=fname)
        self.gpbase.gen_ticks(self.xticks, self.yticks)
        self.gpbase.gen_header(ylabel='Probability')
        print 'plot "%s" with boxes' % self.datafile
