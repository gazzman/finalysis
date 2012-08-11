#!/usr/bin/python
from datetime import datetime
import re
import urllib2

from pytz import timezone

class _DateHelpers(object):
    """A Mixin for cleaning up dates"""
    def _add_timezone(self, date, time='16:00:00', locale='US/Eastern', 
                      fmt='%Y-%m-%d %H:%M:%S'):
        tz = timezone(locale)
        dt = ' '.join([date, time])
        dt = datetime.strptime(dt, fmt)
        tzone = tz.tzname(dt)
        return ' '.join([dt.date().isoformat(), dt.time().isoformat(), tzone])

class _WebHelpers(object):
    """A Mixin for pulling pages from the web"""
    def _pull_page(self, url, header):
        req = urllib2.Request(url, headers=header)
        opened = False
        count = 0
        while not opened:
            if count > 4:
                print ' '.join(['\nOk, we\'re on try', str(count),
                                'now.\nWhy don\'t you see if this url,',
                                url, 'is even working?'])
            try:
                page = urllib2.urlopen(req)
                opened = True
            except urllib2.HTTPError as err:
                if re.match('HTTP Error 404', str(err)):
                    print >> sys.stderr, '...404 problem...waiting 5 sec...',
                    sleep(5)
                    count += 1
                else:
                    raise err
        return page
