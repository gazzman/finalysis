#!/usr/bin/python
__version__ = ".01"
__author__ = "gazzman"
__copyright__ = "(C) 2013 gazzman GNU GPL 3."
__contributors__ = []
from StringIO import StringIO
from datetime import datetime
from time import sleep
import argparse
import csv
import re
import sys
import urllib2

UA = 'Mozilla/5.0(X11; Linux x86_64; rv:10.0.12)Gecko/20130109 Firefox/10.0.12'
HEADER = {'User-Agent' : UA}

class YahooData:
    dfmt = '%Y%m%d'
    url = 'http://ichart.finance.yahoo.com/table.csv?s=%(ticker)s'
    url += '&a=%(fr_month)i&b=%(fr_day)i&c=%(fr_year)i'
    url += '&d=%(to_month)i&e=%(to_day)i&f=%(to_year)i&g=%(info)s&ignore=.csv'

    def grab_data(self, tickerlist, from_date=None, frequency='d'):
        tickerlist
        to = datetime.now()
        if not from_date: fr = datetime.strptime('19000101', self.dfmt)
        else: fr = datetime.strptime(from_date, self.dfmt)
        dates = {'to_year': to.year, 'to_month': to.month-1, 'to_day': to.day,
                 'fr_year': fr.year, 'fr_month': fr.month-1, 'fr_day': fr.day}
        f = open('yprices_' + to.strftime('%Y%m%dT%H%M%S') + '.csv', 'w')
        csvout = csv.DictWriter(f, [])
        firstrun = True
        for ticker in tickerlist:
            print >> sys.stderr, 'Starting %s...' % ticker, 
            divdic = {}
            divinfo = {'ticker': ticker, 'info': 'v'}
            divinfo.update(dates)
            divurl = self.url % divinfo
            divpage = self.pull_page(divurl)
            dividends = self.send_to_csv(divpage)
            for dividend in dividends:
                divdic[dividend['date']] = dividend['dividends']

            priceinfo = {'ticker': ticker, 'info': frequency}
            priceinfo.update(dates)
            priceurl = self.url % priceinfo
            pricepage = self.pull_page(priceurl)
            prices = self.send_to_csv(pricepage)
            if firstrun:
                headers = (['ticker', 'dividends'] + prices.fieldnames)
                f.write(','.join(headers) + '\n')
                csvout.fieldnames = headers
                firstrun = False
            for price in prices:
                price['ticker'] = ticker
                if price['date'] in divdic:
                    price['dividends'] = divdic[price['date']]
                csvout.writerow(price)
            print >> sys.stderr, 'Done!'
        f.close()

    def send_to_csv(self, page):
        s = page.read()
        headers, delim, data = s.partition('\n')
        headers = [x.lower().strip() for x in headers.split(',')]
        return csv.DictReader(StringIO(data), fieldnames=headers)

    def pull_page(self, url):
        req = urllib2.Request(url, headers=HEADER)
        count = 0
        while True:
            if count > 4:
                print ' '.join(["\nOk, we're on try", str(count),
                                "now.\nWhy don't you see if this url,",
                                url, "is even working?"])
            try:
                page = urllib2.urlopen(req)
                return page
            except urllib2.HTTPError, err:
                if re.match('HTTP Error 404', str(err)):
                    print >> sys.stderr, '...404 problem...waiting 5 sec...',
                    sleep(5)
                    count += 1
                else:
                    raise err

# For running from command line
if __name__ == "__main__":
    yd = YahooData()
    description = 'Pull ticker price and dividend data from Yahoo! Finance.'
    from_help = 'Specify date to pull data from. Format is %s'\
                                                   % yd.dfmt.replace('%', '%%')

    p = argparse.ArgumentParser(description=description)
    p.add_argument('tickerfile', type=str, 
                   help='A text file of ticker sybols to pull, one per line.')
    p.add_argument('-v', '--version', action='version', 
                   version='%(prog)s ' + __version__)
    p.add_argument('--fromdate', help=from_help)
    freq_group = p.add_mutually_exclusive_group()
    freq_group.add_argument('--weekly', action='store_true', 
                            help='Pull weekly data')
    freq_group.add_argument('--monthly', action='store_true',
                            help='Pull monthly data')
    args = p.parse_args()

    with open(args.tickerfile, 'r') as f:
        tickers = [x.strip() for x in f.read().split('\n') if x.strip() != '']
    tickers = [x.upper() for x in tickers if x[0] != '#']    

    if args.weekly: freq = 'w'
    elif args.monthly: freq = 'm'
    else: freq = 'd'
    yd.grab_data(tickers, from_date=args.fromdate, frequency=freq)
