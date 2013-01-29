#!/usr/bin/python
import sys

from finalysis.combo_gnuplots import GNUPlotBase

if __name__ == "__main__":
    datfname = sys.argv[1]
    jpgfname = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    increment = sys.argv[5]
    dt = datfname.split('.dat')[0].split('_')[2]
    if float(increment) % 1 == 0:
        start = int(start)
        end = int(end)
        increment = int(increment)
        xtics = range(start, end+increment, increment)
    elif float(increment) == 0.5:
        start = int(float(start)*10.0)
        end = int(float(end)*10.0)
        increment = 5
        strikes = range(start, end+increment, increment)
        xticks = [x/10.0 for x in strikes]
    yticks = [x/10.0 for x in range(0, 11)]
    gpb = GNUPlotBase()
    gpb.xmin = start*.9
    gpb.xmax = end*1.1
    gpb.ymin = 0
    gpb.ymax = 1
    gpb.set_output(fname=jpgfname)
    gpb.gen_header(ylabel='Probability', timestamp=dt)
    gpb.gen_ticks(xticks, yticks)
    print 'plot "%s" with boxes' % datfname
