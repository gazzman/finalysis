import re

sec_form_items = {}

sec_form_items['N-CSR'] = [
    re.compile('\s*item\s+1[^\s\w]?\s+reports\s+to\s+stockholders[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+2[^\s\w]?\s+code\s+of\s+ethics[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+3[^\s\w]?\s+audit\s+committee\s+financial\s+expert[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+4[^\s\w]?\s+principal\s+accountant\s+fees\s+and\s+services[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+5[^\s\w]?\s+audit\s+committee\s+of\s+listed\s+registrants[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+6[^\s\w]?\s+schedule\s+of\s+investments[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+7[^\s\w]?\s+disclosure\s+of\s+proxy\s+voting\s+policies\s+and\s+procedures[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+8[^\s\w]?\s+portfolio\s+managers\s+of\s+closed-end\s+management\s+investment\s+companies[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+9[^\s\w]?\s+purchases\s+of\s+equity\s+securities\s+by\s+closed-end\s+management\s+investment\s+company\s+and\s+affiliated\s+purchasers[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+10[^\s\w]?\s+submission\s+of\s+matters\s+to\s+a\s+vote\s+of\s+security\s+holders[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+11[^\s\w]?\s+controls\s+and\s+procedures[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+12[^\s\w]?\s+exhibits[^\s\w]?\s*', re.UNICODE)
    ]

sec_form_items['N-Q'] = [
    re.compile('\s*item\s+1[^\s\w]?\s+schedule\s+of\s+investments[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+2[^\s\w]?\s+controls\s+and\s+procedures[^\s\w]?\s*', re.UNICODE), 
    re.compile('\s*item\s+3[^\s\w]?\s+exhibits[^\s\w]?\s*', re.UNICODE)
    ]
