#!/usr/bin/python
class ButterflyHistogram():
    outfile = 'butterhist.dat'
    intervals = list()
    prices = list()
    probs = list()
    price_sum = 0
    n = int()

    def __init__(self, strike_intervals):
        self.intervals = strike_intervals

    def midpoint(self, interval):
        return (interval[1] - interval[0])/2.0
        
    def sum_prices(self):
        self.price_sum = 0
        for x in prices: self.price_sum += x

    def calculate_probabilities(self):
        self.sum_prices()
        f = open(outfile, 'w')
        for i in range(0, n+3):
            if i == 0 or i == n+2: probs[i] = 0
            elif i == 1 or i == n+1: probs[i] = 0.5*prices[i]/self.price_sum
            else: probs[i] = 0.5*(prices[i-1] + prices[i])/self.price_sum
            f.write('%0.2f %0.3f' % (self.midpoint(interval[i]), probs[i]))
        f.close()

    def plot_histogram(self):
        print 'plot "%s" with boxes' % self.outfile
