#!/usr/bin/env python
"""sec.py: A module for parsing SEC filings.

UASTRING          -- the uastring you want to use.
HEADER            -- the header info passed to urllib2.Request object
DATAPATH          -- defaults to current working directory
SERVER            -- the web address of the sec website
DATEFORMAT        -- for moving between strings, datetime, 
                     and struct_time objects
TABLE_ID          -- the table attribute and value used for parsing 
                     the url of the entire text submission url
COL_TEXT          -- the column text that contains the anchor for 
                     the url of the entire text submission url
Submission        -- a named tuple that stores submission rss 
                     feed information
SubmissionHandler -- the class that takes a cik and parses submissions 
                     into text documents

"""
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) gazzman GNU GPL 3."
__contributors__ = []

from collections import namedtuple
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
from datetime import datetime
import codecs
import pickle
import sys
import re
import time
import urllib2

from BeautifulSoup import BeautifulSoup
import feedparser
import html2text


UASTRING=('Mozilla/5.0 (X11; Linux x86_64; rv:10.0.5) Gecko/20120606'
            + 'Firefox/10.0.5')
HEADER = {'User-Agent' : UASTRING}
DATAPATH = './'
SERVER = 'http://sec.gov'
DATEFORMAT = '%Y-%m-%dT%H:%M:%S'
TABLE_ID = {'summary' : 'Document Format Files'}
COL_TEXT = 'Complete submission text file'

feedparser.USER_AGENT = UASTRING
html2text.IGNORE_ANCHORS = True
html2text.IGNORE_IMAGES = True
html2text.IGNORE_EMPHASIS = True

Submission = namedtuple('Submission', ['date', 'title', 'sub_url', 'form'])

class SubmissionGetter:
    """SubmissionGetter: Gets complete sumbission text from SEC

    A SubmissionHandler is initialized with the entity's cik integer 
    and a list of strings denoting form names. Various functions are
    implemented to obtain information about where a sumbission is 
    located and how far back to go to retrieve them.

    """

    def __init__(self, cik, forms=[]):
        self.cik = cik
        self.forms = forms
        self.sub_urlfile = str(cik) + '_submissions.pkl'
        try:
            self.submissions = pickle.load(open(self.sub_urlfile, 'rb'))
        except IOError as err:
            if re.search('No such file', str(err)):
                message = ('No pickled submission url data.'
                           + ' Run \'pull_submission_urls\' to populate list.')
                print >> sys.stderr, message
                self.submissions = OrderedDict()
            else:
                raise err

    def feed_url(self, cik, start):
        return (SERVER 
                + '/cgi-bin/browse-edgar?action=getcompany&CIK=' 
                + str(cik) 
                + '&type=&dateb=&owner=exclude&start='
                + str(start) + '&count=100&output=atom')

    def get_submission_urls(self):
        return self.submissions

    def match_form(self, sub_form):
        if len(self.forms) == 0:
            return True
        else:
            for form in self.forms:
                if re.match(form.lower(), sub_form.lower()):
                    return True
        return False

    def pull_submission_urls(self, refresh=False, verbose=False):
        """Parse the rss feed for the cik and store the urls,
           titles, and dates

        Keyword arguments:
        refresh -- will refresh from web even if data had been pickled
        verbose -- Print extra information

        """
        if len(self.submissions) == 0 or refresh:
            start = 0
            d = feedparser.parse(self.feed_url(self.cik, start))
            while len(d.entries) > 0:
                if verbose:
                    print self.feed_url(self.cik, start)
                start += 100
                for e in d.entries:
                    title = e.title.encode('UTF-8')
                    form = title.partition(' ')[0]
                    url = e.link
                    date = (time.strftime(DATEFORMAT, e['updated_parsed']) 
                            + '+00:00')
                    s = Submission(date, title, url, form)
                    if s not in self.submissions:
                        self.submissions[s] = None
                d = feedparser.parse(self.feed_url(self.cik, start))
            self.submissions = OrderedDict(sorted(self.submissions.items(), 
                                                  key=lambda d: d[0].date, 
                                                  reverse=True))
            pickle.dump(self.submissions, open(self.sub_urlfile, 'wb'))
        elif verbose:
            outmsg = ('Already pulled submission urls.'
                     + ' Pass \'refresh=True\' to refresh anew.')
            print >> sys.stderr, outmsg
        print >> sys.stderr, 'Submission URLs pulled.'
        message = ('Call \'pull_text_filing\' to pull, parse, and store the'
                   + ' complete submissiond as text files.')
        print >> sys.stderr, message

    def pull_text_filing(self, count=None, verbose=False, 
                         refresh_url=False, refresh_file=False):
        """Get and store the text of the complete submission filing.

        Keyword arguments:
        count        -- the number of form submission urls to pull
        verbose      -- prints extra info if True
        refresh_url  -- will refresh url info from web even if pickled
        refresh_file -- will refresh file from web even if pickled

        """
        matchcount = 0
        for submission in self.submissions:
            matches = self.match_form(submission.form)
            already_pulled = False

            if matches:
                matchcount += 1
                sub_tfile = '_'.join([str(self.cik), submission.form, 
                                      submission.date]) + '.txt'
                sub_tfile = sub_tfile.replace('/', '')
                if self.submissions[submission] is None or refresh_url:
                    if verbose:
                        message = 'Pulling text_url from ' + submission.sub_url
                        print >> sys.stderr, message
                    req = urllib2.Request(submission.sub_url, headers=HEADER)
                    page = urllib2.urlopen(req)
                    soup = BeautifulSoup(page)

                    doctable = soup.find('table', TABLE_ID)
                    doctable_tr = doctable.findAll('tr')
                    for tr in doctable_tr:
                        tds = tr.findAll('td')
                        for td in tds:
                            if re.search(COL_TEXT, td.text):
                                self.submissions[submission] = (SERVER 
                                                                + tr.a['href'])
                elif self.submissions[submission] is not None:
                    if verbose:
                        message = ('Already pulled text_url from '
                                   + submission.sub_url +' Pass'
                                   + ' \'refresh_url=True\' to refresh anew.')
                        print >> sys.stderr, message

                try:
                    codecs.open(sub_tfile, 'r', encoding='UTF-8')
                    already_pulled = True
                except IOError as err:
                    if re.search('No such file', str(err)):
                        message = ('No stored submission text file for '
                                   + ' '.join([submission.form, 
                                               submission.date])
                                   + '\nGrabbing new data from '
                                   + self.submissions[submission])
                        print >> sys.stderr, message
                    else:
                        raise err

                if not already_pulled or refresh_file:
                    req = urllib2.Request(self.submissions[submission], 
                                          headers=HEADER)
                    page = urllib2.urlopen(req)
                    print >> sys.stderr, 'Pouring the soup...'
                    soup = BeautifulSoup(page, convertEntities=BeautifulSoup.HTML_ENTITIES)

                    sec_header = soup.find('sec-header')
                    if sec_header is None:
                        sec_header = soup.find('ims-header')
                    sec_header = sec_header.prettify().decode('UTF-8')

                    sec_doc = soup.find('document')
                    if sec_doc.html is not None:
                        print >> sys.stderr, 'Converting to text...'
                        sec_doc = html2text.html2text(sec_doc.prettify().decode('UTF-8'))
                    else:
                        sec_doc = sec_doc.prettify().decode('UTF-8')

                    print >> sys.stderr, 'Writing to ' + sub_tfile 
                    sub_text = '\n'.join([sec_header, sec_doc])

                    with codecs.open(sub_tfile, 'w', encoding='UTF-8') as ofl:
                        ofl.write(sub_text)
                elif verbose:
                    message = ('Already pulled raw text for '
                               + ' '.join([submission.form, submission.date]))
                    print >> sys.stderr, message

            if matchcount == count:
                break
        pickle.dump(self.submissions, open(self.sub_urlfile, 'wb'))
