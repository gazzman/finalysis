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
SubmissionGetter  -- the class that takes a cik and parses submissions 
                     into text documents

"""
__version__ = ".01"
__author__ = "gazzman"
__copyright__ = "(C) gazzman GNU GPL 3."
__contributors__ = []

from collections import namedtuple
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
from datetime import datetime
from itertools import product
import codecs
import locale
import pickle
import sys
import re
import time
import urllib2

from BeautifulSoup import BeautifulSoup
from html2text import html2text
import feedparser

from sec_form_dictionaries import sec_form_items, Data


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

locale.setlocale(locale.LC_NUMERIC, '')

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
                   + ' complete submission as text files.')
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
        filenames = []
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
                    soup = BeautifulSoup(page, 
                        convertEntities=BeautifulSoup.HTML_ENTITIES)

                    sec_header = soup.find('sec-header')
                    if sec_header is None:
                        sec_header = soup.find('ims-header')
                    sec_header = sec_header.prettify().decode('UTF-8')

                    sec_doc = soup.find('document')
                    if sec_doc.html is not None:
                        print >> sys.stderr, 'Converting to text...'
                        sec_doc = html2text.html2text(
                            sec_doc.prettify().decode('UTF-8'))
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

                filenames.append(sub_tfile)

            if matchcount == count:
                break
        pickle.dump(self.submissions, open(self.sub_urlfile, 'wb'))
        return filenames

class CIKFinder():
    def __init__(self, filename='cikmap.pkl'):
        self.filename = filename
        try:
            self.cik_dict = pickle.load(open(self.filename, 'rb'))
        except IOError:
            msg = 'No pickled cik file found, generating new pickle file.'
            print >> sys.stdout, msg
            self.cik_dict = {}

    def get_cik(self, symbol):
        if symbol not in self.cik_dict:
            url = (SERVER + '/cgi-bin/browse-edgar?company=&match=&CIK=' + 
                   symbol + '&filenum=&State=&Country=&SIC=&owner=exclude&' + 
                   'Find=Find+Companies&action=getcompany')
            req = urllib2.Request(url, headers=HEADER)
            page = urllib2.urlopen(req)
            soup = BeautifulSoup(page)
            info = soup.find(attrs={'class': 'companyName'})
            info = info.getText().partition('CIK#:')[2]
            self.cik_dict[symbol] = int(info.split(' ')[0])
            pickle.dump(self.cik_dict, open(self.filename, 'wb'))
        return self.cik_dict[symbol]

class SubmissionParser():
    '''A class to parse submission text'''
    def __init__(self, filename, symbol):
        '''Parses the text file using the following steps:'''
        # Step 1: Store text as list of lines w/data
        l = []
        with open(filename, 'r') as f:
            for line in f:
                if line != '\n':
                    l.append(line.decode('utf-8').strip())

        # Step 2: Find header type
        if l.count('<sec-header>') > 0:
            (hstart, hend) = ('<sec-header>', '</sec-header>')
            self.header_type = 'sec'
        elif l.count('<ims-header>') > 0:
            (hstart, hend) = ('<ims-header>', '</ims-header>')
            self.header_type = 'ims'
        else:
            raise BaseException('This form has no header!')

        # Step 3: Split into header and text and parse header
        self.header = l[l.index(hstart):l.index(hend)+1]
        self.series_dict = self.parse_sec_header()
        (self.name, self.class_) = self.find_name(symbol)
        self.text = l[l.index(hend)+1:]

    def try_convert_to_num(self, string):
        try:
            return locale.atof(string)
        except ValueError:
            if '$' in string:
                return self.try_convert_to_num(string.replace('$', ''))
            else:
                return string

    def mark(self, x, pattern_dict, verbose=False):
        for pattern in pattern_dict:
            if re.search(pattern, x.lower()):
                if verbose: print x
                return pattern_dict[pattern]
        return self.try_convert_to_num(x)

    def get_section(self, symbol, section_start, section_end, 
                    name_override, pagebreak_override, verbose=False):
        '''Return a targeted section of the submission as a list

        symbol  -- the ticker symbol string
        section_start
        section_end
        name_override
        pagebreak_override
            -- two-member lists of the form
                    [string, pattern]
                so that a list element that matches the pattern
                will be replaced with the string.

        A section is delimited by the section_start and section_end
        patterns.

        The section that contains the name on the same page as the
        section_start is the one returned.

        The default behavior is to use '* * *' as the pagebreak, the
        fund name from the SEC filing as the name, and the section 
        'Common Stocks' to 'Total Common Stocks'.

        '''
        if verbose: print self.name

        pattern_dict = {}
        if len(name_override) == 2:
            fund_name = name_override[0]
            fund_pattern = name_override[1]
        else:
            fund_name = ' '.join(self.name)
            fund_pattern = ''.join(['^', '\s+'.join(self.name), '$'])
        fund_pattern = re.compile(fund_pattern.lower(), re.UNICODE)
        pattern_dict[fund_pattern] = fund_name

        if len(pagebreak_override) == 2:
            pagebreak = pagebreak_override[0]
            pagebreak_pattern = pagebreak_override[1]
        else:
            pagebreak = u'* * *'
            pagebreak_pattern = '^\*\s+\*\s+\*$'
        pagebreak_pattern = re.compile(pagebreak_pattern.lower(), re.UNICODE)
        pattern_dict[pagebreak_pattern] = pagebreak

        if len(section_start) == 2:
            start = section_start[0]
            start_pattern = section_start[1]
        else:
            start = 'section_start'
            start_pattern = '^common\s+stocks\s*[^\s\w]?\s*?(?!.*continued)'
        start_pattern = re.compile(start_pattern.lower(), re.UNICODE)
        pattern_dict[start_pattern] = start

        if len(section_end) == 2:
            end = section_end[0]
            end_pattern = section_end[1]
        else:
            end = 'section_end'
            end_pattern = '^total\s+common\s+stocks'
        end_patern = re.compile(end_pattern.lower(), re.UNICODE)
        pattern_dict[end_pattern] = end

        m = map(lambda x: self.mark(x, pattern_dict, verbose), self.text)
        og_list = m
        m.reverse()

        if verbose:
            print 'Funds in sub:', len(self.series_dict)
            print 'Fund names:', m.count(fund_name)
            print 'Section starts:', m.count(start)
            print 'Section ends:', m.count(end)
            print 'Pagebreaks:', m.count(pagebreak)

        try:
            if len(self.series_dict) > 1:
                found = False
                while not found:
                    end_idx = m.index(end)
                    start_idx = m[end_idx:].index(start) + end_idx
                    page_idx = m[start_idx:].index(pagebreak) + start_idx
                    (cand, m) = (m[end_idx:page_idx+1], m[page_idx+1:])
                    print 'end', end_idx
                    print 'start', start_idx
                    print 'pagebreak', page_idx
                    if fund_name in cand: found = True
            else:
                end_idx = m.index(end)
                start_idx = m[end_idx:].index(start) + end_idx
                print end_idx, start_idx
                cand = m[end_idx:start_idx+1]
        except ValueError as err:
            return (og_list, None)
        cand.reverse()
        if verbose: print cand
        return (og_list, cand)

    def find_name(self, symbol):
        for series in self.series_dict:
            (name, contract_dict) = self.series_dict[series][-2:]
            for contract in contract_dict:
                (con_class, con_symbol) = contract_dict[contract]
                if symbol == con_symbol:
                    return (name.lower().split(), con_class.lower().split())
        return (None, None)

    def parse_sec_header(self):
        '''
        This function parses the SEC header for series and contract info.
        It returns a dictionary of series ID keys whose values are series
        info and a dictionary of contract ID keys whose values consist of
        tuples containing various contract info. The following is a 
        schematic:

                self.series_dict[sid] = (sownercik, sname, cdic)
                    cdic[cid] = (cname, csymbol)

        It omits contracts that have no symbol.
        '''
        soup = BeautifulSoup('\n'.join(self.header))

        header = soup.find(''.join([self.header_type, '-header']))
        series = header.findAll('series')
        series_dict = {}
        for s in series:
            contracts = s.findAll('class-contract')
            cdic = {}
            for c in contracts:
                try:
                    csymbol = c.find(
                        'class-contract-id').find(
                            'class-contract-name').find(
                                'class-contract-ticker-symbol').extract().text
                except AttributeError as err:
                    csymbol = None
                cname = c.find(
                    'class-contract-id').find(
                        'class-contract-name').extract().text
                cid = c.find('class-contract-id').extract().text
                cdic[cid] = (cname, csymbol)

            sname = s.find('series-name').extract().text
            sid = s.find('series-id').extract().text
            sownercik = s.find('owner-cik').extract().text
            series_dict[sid] = (sownercik, sname, cdic)
        return series_dict

    def find_item_locations(self, items):
        '''Find indexes where items live'''
        self.item_locations = {}
        for item in items:
            m = map(lambda x: bool(re.search(items[item].pattern, x.lower())),
                    self.text)
            while m.count(True) > 0:
                idx = m.index(True)
                items[item].locations.append(idx)
                m[idx] = False

    def find_toc(self):
        toc = re.compile('table\s+of\s+contents', re.UNICODE)
        print self.text.count('#####')
        print self.text.count('* * *')

    def gen_page_dict(self):
        self.pages = {}
        end = 0
        while self.l[end:].count('#####') > 0:
            try:
                start = self.l[end:].index('#####') + end
                end = self.l[start:].index('* * *') + start
                if re.search('^\d+$', self.l[end-1]):
                    self.pages[int(self.l[end-1])] = (int(start), int(end))
                elif re.search('^\d+$', l[end-2]):
                    self.pages[int(self.l[end-2])] = (int(start), int(end))
            except ValueError:
                break
