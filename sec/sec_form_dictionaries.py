from collections import namedtuple
try: from collections import OrderedDict # >= 2.7
except ImportError: from ordereddict import OrderedDict # 2.6
import re

Data = namedtuple('Data', ['pattern', 'locations'])

sec_form_items = {}

sec_form_items['N-CSR'] = OrderedDict({
    'item 1': Data(re.compile(('\s*item\s+1\s*[^\s\w]?\s+reports\s+to' + 
                               '\s+stockholders' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 2': Data(re.compile(('\s*item\s+2\s*[^\s\w]?\s+code\s+of\s+ethics' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 3': Data(re.compile(('\s*item\s+3\s*[^\s\w]?\s+audit\s+committee' + 
                               '\s+financial\s+expert' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 4': Data(re.compile(('\s*item\s+4\s*[^\s\w]?\s+principal' + 
                               '\s+accountant\s+fees\s+and\s+services' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 5': Data(re.compile(('\s*item\s+5\s*[^\s\w]?\s+audit\s+committee' + 
                               '\s+of\s+listed\s+registrants' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 6': Data(re.compile(('\s*item\s+6\s*[^\s\w]?\s+schedule\s+of' + 
                               '\s+investments' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 7': Data(re.compile(('\s*item\s+7\s*[^\s\w]?\s+disclosure\s+of' + 
                               '\s+proxy\s+voting\s+policies\s+and' + 
                               '\s+procedures' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 8': Data(re.compile(('\s*item\s+8\s*[^\s\w]?\s+portfolio' + 
                               '\s+managers\s+of\s+closed-end\s+management' + 
                               '\s+investment\s+companies' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
#   'item 9': Data(re.compile(('\s*item\s+9\s*[^\s\w]?\s+purchases\s+of' + 
#                              '\s+equity\s+securities\s+by\s+closed-end' + 
#                              '\s+management\s+investment\s+company\s+and' + 
#                              '\s+affiliated\s+purchasers' + 
#                              '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 9': Data(re.compile(('\s*item\s+9\s*[^\s\w]?\s+purchases\s+of' + 
                               '\s+equity\s+securities\s+by\s+closed-end' + 
                               '\s+management' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
#    'item 10': Data(re.compile(('\s*item\s+10\s*[^\s\w]?\s+submission\s+of' + 
#                           '\s+matters\s+to\s+a\s+vote\s+of\s+security' + 
#                           '\s+holders' + 
#                           '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 10': Data(re.compile(('\s*item\s+10\s*[^\s\w]?\s+submission\s+of' + 
                                '\s+matters' + 
                                '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 11': Data(re.compile(('\s*item\s+11\s*[^\s\w]?\s+controls\s+and' + 
                                '\s+procedures' + 
                                '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 12': Data(re.compile(('\s*item\s+12\s*[^\s\w]?\s+exhibits' + 
                                '\s*[^\s\w]?\s*'), re.UNICODE), list())
    })

sec_form_items['N-Q'] = OrderedDict({
    'item 1': Data(re.compile(('\s*item\s+1\s*[^\s\w]?\s+schedule\s+of' + 
                               '\s+investments' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 2': Data(re.compile(('\s*item\s+2\s*[^\s\w]?\s+controls\s+and' + 
                               '\s+procedures' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list()), 
    'item 3': Data(re.compile(('\s*item\s+3\s*[^\s\w]?\s+exhibits' + 
                               '\s*[^\s\w]?\s*'), re.UNICODE), list())
    })
    
industries = [
    Data(re.compile('information\s+technology', re.UNICODE), list()), 
    Data(re.compile('financials', re.UNICODE), list()), 
    Data(re.compile('consumer\s+discretionary', re.UNICODE), list()), 
    Data(re.compile('health\s+care', re.UNICODE), list()), 
    Data(re.compile('industrials', re.UNICODE), list()), 
    Data(re.compile('energy', re.UNICODE), list()), 
    Data(re.compile('consumer\s+staples', re.UNICODE), list()), 
    Data(re.compile('materials', re.UNICODE), list()), 
    Data(re.compile('utilities', re.UNICODE), list()), 
    Data(re.compile('telecommunication\s+services', re.UNICODE), list()), 
    Data(re.compile('short\s?[^\s\w]?\s?term', re.UNICODE), list())
    ]

asset_types = [
    Data(re.compile('common\s+stocks', re.UNICODE), list())
#    Data(re.compile('preferred\s+stocks', re.UNICODE), list()), 
#    Data(re.compile('warrants', re.UNICODE), list()), 
#    Data(re.compile('bonds', re.UNICODE)
#    Data(re.compile('other', re.UNICODE)
    ]
