#!/usr/bin/python
import argparse
import sys

class GNUPlotBase():
    xmin = 0
    xmax = 100
    ymin = -10
    ymax = 10

    def set_output(self, fname=None):
        commands = []
        commands.append('\n#Plot Output')
        commands.append('unset multiplot')
        if not fname: commands.append('set terminal x11')
        else:
            commands.append('set terminal jpeg font cour 6 size 800,600 enhanced')
#            commands.append('set terminal postscript eps enhanced size 5.5in, 4.25in')
            commands.append('set output "%s"' % fname)
        return '\n'.join(commands)

    def gen_ticks(self, xticklist, yticklist):
        commands = []
        xticks = [str(x) for x in xticklist]
        yticks = [str(x) for x in yticklist]
        commands.append('\n#Set ticks')
        commands.append('unset xtics')
        commands.append('unset ytics')
        commands.append('set xtics (%s)' % ', '.join(xticks))
        commands.append('set ytics (%s)' % ', '.join(yticks))
        return '\n'.join(commands)

    def gen_header(self, xlabel='Spot Price', ylabel='Payoff at Expiry',
                   timestamp=None):
        commands = []
        commands.append('\n#Chart Settings')
        if not timestamp: commands.append('set timestamp "%Y-%m-%dT%H:%M:%S"')
        else: commands.append('set timestamp "%s"' % timestamp)
        commands.append('xmin = %0.3f' % self.xmin)
        commands.append('xmax = %0.3f' % self.xmax)
        commands.append('ymin = %0.3f' % self.ymin)
        commands.append('ymax = %0.3f' % self.ymax)
        commands.append('set xrange [xmin:xmax]')
        commands.append('set yrange [ymin:ymax]')

        commands.append('set key off')
        commands.append('set grid back lt 0 lc rgb "grey"')

        commands.append('set xlabel "%s"' % xlabel)
        commands.append('set ylabel "%s"' % ylabel)

        commands.append('set parametric')
        commands.append('set multiplot')
        return '\n'.join(commands)

class GNUPlotOption(GNUPlotBase):
    def __init__(self, qty, right, strike):
        self.qty = int(qty)
        self.right = right.upper()
        self.strike = float(strike)
        if right == 'C':
            self.otm_interval = (0, self.strike)
            self.itm_interval = (self.strike, sys.maxint)
            self.itm_payoff = '%i*(t-%0.3f)' % (self.qty, self.strike)
        if right == 'P':
            self.otm_interval = (self.strike, sys.maxint)
            self.itm_interval = (0, self.strike)
            self.itm_payoff = '%i*(%0.3f-t)' % (self.qty, self.strike)

    def is_itm_at(self, spot):
        return spot > self.itm_interval[0] and spot < self.itm_interval[1]

    def payoff_at(self, spot):
        if is_itm_at(spot):
            if self.right == 'C': return spot - self.strike
            if self.right == 'P': return self.strike - spot
        else: return 0

    def plot_option_payoff(self, color='blue'):
        heading = '\n#Option Payoff'
        otm = 'plot [t=%0.3f:%0.3f] t, 0 lc rgb "%s" lt 2'\
                  % (self.otm_interval + (color,))
        itm = 'plot [t=%0.3f:%0.3f] t, %s lc rgb "%s"'\
                  % (self.itm_interval + (self.itm_payoff, color))
        return '\n'.join([heading, otm, itm])

class GNUPlotCombo(GNUPlotBase):
    def plot_combo_payoff(self, options, color):
        commands = []
        strikes = dict([(x.strike, None) for x in options]).keys()
        strikes += [0, self.xmax]
        strikes.sort()
        strike_intervals = zip(strikes[:-1], strikes[1:])
        plotstr = 'plot [t=%0.3f:%0.3f] t, %s lc rgb "%s"' 
        for interval in strike_intervals:
            mid = (interval[1] + interval[0])/2
            payoff = ['0']
            payoff += [x.itm_payoff for x in options if x.is_itm_at(mid)]
            print payoff
            commands.append(plotstr % (interval + ('+'.join(payoff), color)))
        return '\n'.join(commands)

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

if __name__ == "__main__":
    description = 'A utility for plotting option combo expiry payoffs.'
    ofile_help = 'A space-delimited file of options to plot, one per line. '
    ofile_help += "The format of each line is 'qty right strike'"

    p = argparse.ArgumentParser(description=description)
    p.add_argument('options_file', type=str, help=ofile_help)
    args = p.parse_args()

    try:
        from Gnuplot import Gnuplot
    except ImportError:
        msg = 'Continuing without Gnuplot.py. Sending commands to stdout'
        print >> sys.stderr, msg

    f = open(args.options_file, 'r')
    options = []
    for line in f:
        try:
            if line.strip()[0] != '#':
                options.append(GNUPlotOption(*line.split()))
        except IndexError:
            pass
    strikes = [x.strike for x in options]
    strikes.sort()
    c = GNUPlotCombo()
    c.xmin = min(strikes)*.9
    c.xmax = max(strikes)*1.1
    spot0_payoff = sum([x.qty*x.strike for x in options if x.right == 'P'])
    spotinf_payoff = sum([x.qty*(c.xmax-x.strike) for x in options if x.right == 'C'])
    try:
        c.ymax = max(2*max([x[1] - x[0] for x in zip(strikes[:-1], strikes[1:])]),
                     spot0_payoff, spotinf_payoff, 1)
        c.ymin = min(2*max([x[1] - x[0] for x in zip(strikes[:-1], strikes[1:])]),
                     spot0_payoff, spotinf_payoff, -1)
        print c.ymax, c.ymin
    except ValueError:
        c.ymax = 1
        c.ymin = -1
#    if c.ymax > c.xmax or c.ymin < c.xmin:
#        c.ymax = 2*max([x[1] - x[0] for x in zip(strikes[:-1], strikes[1:])])
#        c.ymin = -1*c.ymax
    tics = dict(zip(strikes, [None]*len(strikes))).keys()
    tics.sort()
    if len(tics) == 1:
        xtics = [tics[0] - 1, tics[0], tics[0] + 1]
        c.ymax = max(xtics) - xtics[1]
        c.ymin = -1*c.ymax
    else:
        xtics = [c.xmin, min(strikes), c.xmax, max(strikes)]\
               + [x for x in tics if tics.index(x) % 2 == 0]
    ytics = [c.ymin + (c.ymax-c.ymin)/10.0*x for x in range(0, 11)]
    c.ymax = max(ytics)*1.1
    c.ymin = min(ytics)*1.1 

    the_plot = []
    the_plot.append(c.set_output())
    the_plot.append(c.gen_header())
    the_plot.append(c.gen_ticks(xtics, ytics))
    for option in options:
        if option.qty  < 0:
            the_plot.append(option.plot_option_payoff(color='red'))
        elif option.qty > 0:
            the_plot.append(option.plot_option_payoff(color='blue'))
    the_plot.append(c.plot_combo_payoff(options, 'black'))
    the_plot = '\n'.join(the_plot)
    try:
        g = Gnuplot()
        g(the_plot)
        raw_input('Press ENTER to close plot')
    except NameError:
        print the_plot
