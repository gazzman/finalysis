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
        xtics = range(start, end+increment, increment*2)
    elif float(increment) == 0.5:
        start = int(float(start)*10.0)
        end = int(float(end)*10.0)
        increment = 5
        strikes = range(start, end+increment, increment*2)
        xticks = [x/10.0 for x in strikes]
    gpb = GNUPlotBase()
    gpb.xmin = start*.9
    gpb.xmax = end*1.1
    gpb.ymin = 0
    gpb.ymax = xticks[1] - xticks[0]
    yticks = [float(gpb.ymax)/5.0*x for x in range(0, 6)]
    gpb.set_output(fname=jpgfname)
    gpb.gpbase.gen_header(xlabel='Spot Price at Expiry',
                          ylabel='Butterfly Price',
                          timestamp=timestamp)
    gpb.gen_ticks(xticks, yticks)
    print 'plot "%s" with boxes' % datfname
