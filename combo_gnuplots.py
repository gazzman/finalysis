#!/usr/bin/python
import sys

class GNUPlotBase():
    xmin = 0
    xmax = 100
    ymin = -10
    ymax = 10

    def set_output(self, fname=None):
        print '\n#Plot Output'
        print 'unset multiplot'
        if not fname: print 'set terminal x11'
        else:
            print 'set terminal postscript eps enhanced size 5.5in, 4.25in'
            print 'set output "%s"' % fname

    def gen_header(self):
        print '\n#Chart Settings'
        print 'xmin = %0.3f' % self.xmin
        print 'xmax = %0.3f' % self.xmax
        print 'ymin = %0.3f' % self.ymin
        print 'ymax = %0.3f' % self.ymax
        print 'set xrange [xmin:xmax]'
        print 'set yrange [ymin:ymax]'

        print 'set key off'
        print 'set grid back lt 0 lc rgb "grey"'

        print 'set xlabel "Spot Price"'
        print 'set ylabel "Payoff at Expiry"'

        print 'set parametric'
        print 'set multiplot'

    def gen_ticks(self, xticklist, yticklist):
        xticks = [str(x) for x in xticklist]
        yticks = [str(x) for x in yticklist]
        print '\n#Set ticks'
        print 'unset xtics'
        print 'unset ytics'
        print 'set xtics (%s)' % ', '.join(xticks)
        print 'set ytics (%s)' % ', '.join(yticks)

class GNUPlotOption(GNUPlotBase):
    def __init__(self, qty, right, strike):
        qty = int(qty)
        right = right.upper()
        self.strike = float(strike)
        if right == 'C':
            self.otm_interval = (0, self.strike)
            self.itm_interval = (self.strike, sys.maxint)
            self.itm_payoff = '%i*(t-%0.3f)' % (qty, self.strike)
        if right == 'P':
            self.otm_interval = (self.strike, sys.maxint)
            self.itm_interval = (0, strike)
            self.itm_payoff = '%i*(%0.3f-t)' % (qty, self.strike)

    def is_itm_at(self, spot):
        return spot > self.itm_interval[0] and spot < self.itm_interval[1]

    def plot_option_payoff(self, color='blue'):
        heading = '\n#Option Payoff'
        otm = 'plot [t=%0.3f:%0.3f] t, 0 lc rgb "%s" lt 2'\
                  % (self.otm_interval + (color,))
        itm = 'plot [t=%0.3f:%0.3f] t, %s lc rgb "%s"'\
                  % (self.itm_interval + (self.itm_payoff, color))
        print '\n'.join([heading, otm, itm])

class GNUPlotCombo(GNUPlotBase):
    def plot_combo_payoff(self, options, color):
        strikes = dict([(x.strike, None) for x in options]).keys()
        strikes += [0, self.xmax]
        strikes.sort()
        strike_intervals = zip(strikes[:-1], strikes[1:])
        plotstring = 'plot [t=%0.3f:%0.3f] t, %s lc rgb "%s"' 
        for interval in strike_intervals:
            mid = (interval[1] + interval[0])/2
            payoff = ['0']
            payoff += [x.itm_payoff for x in options if x.is_itm_at(mid)]
            print plotstring % (interval + ('+'.join(payoff), color))

class GNUPlotButterfly(GNUPlotCombo):
    def __init__(self, K1, K2, K3, rights='C'):
        K1 = float(K1)
        K2 = float(K2)
        K3 = float(K3)
        self.rights = rights.upper()
        self.lwing = GNUPlotOption(1, rights, K1)
        self.body = GNUPlotOption(-2, rights, K2)
        self.rwing = GNUPlotOption(1, rights, K3)

    def plot_legs(self):
        self.lwing.plot_option_payoff(color='blue')
        self.body.plot_option_payoff(color='red')
        self.rwing.plot_option_payoff(color='blue')

    def plot_payoff(self, color='black'):
        self.plot_combo_payoff([self.lwing, self.body, self.rwing], color)

class GNUPlotSpread(GNUPlotCombo):
    def __init__(self, option1, option2):
        self.option1 = option1
        self.option2 = option2

    def plot_legs(self, color1='blue', color2='red'):
        self.option1.plot_option_payoff(color1)
        self.option2.plot_option_payoff(color2)

    def plot_payoff(self, color='black'):
        self.plot_combo_payoff([self.option1, self.option2], color)
