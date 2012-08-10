#!/usr/bin/env python
"""gen_weight_file:
An interactive script that generates a portfolio weight file for
use with the compute_portfolio_returns module
"""
__version__ = ".00"
__author__ = "gazzman"
__copyright__ = "(C) gazzman GNU GPL 3."
__contributors__ = []

from datetime import datetime
import re
import sys

DEFAULT_OUTFILE = 'portfolio.csv'
DISPLAY_DATE_FORMAT = 'YYYY-MM-DD'
DATE_FORMAT = '%Y-%m-%d'

########################################################################
# BEGIN function definitions
########################################################################
def _ask(question):
    confirmed = False
    while not confirmed:
        response = raw_input(question)
        (response, confirmed) = _eval_yn(response)
    return response

def _confirm():
    return _ask('Is this correct (Y/n)? ')

def _eval_yn(yn):
    if yn.strip() == '' or re.match('y', yn.lower()):
        return (True, True)
    elif re.match('n', yn.lower()):
        return (False, True)
    else:
        print 'Please indicate either \'Y\'es or \'N\'o.'
        return ('', False)

def _get_number(question):
    correct_type = False
    while not correct_type:
        try:
            value = float(raw_input(question))
            correct_type = True
        except ValueError:
            print 'You\'ve got to enter a number.'
    return value    

def _get_date(question):
    correct_type = False
    while not correct_type:
        value = raw_input(question)
        try:
            value = datetime.strptime(value, DATE_FORMAT)
            correct_type = True
        except ValueError:
            print '\t' + value + ' is not a valid date!'
            print ('You\'ve got to enter the date in the form ' 
                   + DISPLAY_DATE_FORMAT)
    return value

def _gen_interval(num):
    interval = []
    correct = False
    while not correct:
        compute_weights = _ask('Compute weights from market values (Y/n)? ')
        if compute_weights:
            totvalue = _get_number('Enter starting portfolio value: ')
            print 'The value is', totvalue
            correct = _confirm()
        else:
            print 'You chose to enter the weights directly.'
            correct = _confirm()

    correct = False
    while not correct:
        f_date = _get_date('Enter starting date (' 
                           + DISPLAY_DATE_FORMAT + '): ')
        t_date = _get_date('Enter ending date (' 
                           + DISPLAY_DATE_FORMAT + '): ')
        if f_date < t_date:
            f_date = f_date.date().isoformat()
            t_date = t_date.date().isoformat()
            print ('The period is from ' + f_date + ' to ' + t_date)
            correct = _confirm()
        else:
            print ('\t' + f_date.date().isoformat() + ' is not before ' 
                   + t_date.date().isoformat() + '!\nTry again!')
    weights = []
    interval_has_more = True
    while interval_has_more:
        correct = False
        while not correct:
            ticker = raw_input('Enter ticker' +
                               ' (or press enter if done): ').upper()
            if ticker.strip() == '':
                interval_has_more = False
                break
            if compute_weights:
                ticvalue = _get_number('Enter starting ticker value: ')
                weight = ticvalue/totvalue
                print 'You entered', ticker, ticvalue
                correct = _confirm()
            else:
                weight = _get_number('Enter starting weight: ')
                print 'You entered', ticker, weight
                correct = _confirm()
        if interval_has_more:
            weights.append(weight)
            interval.append(','.join([f_date, t_date, ticker, str(weight)]))
            print '\nInterval', str(num), 'composition so far:'
            print '\n'.join(interval)
            print '\nWeights sum to', sum(weights)

    if sum(weights) != 1:
        print '\nWARNING: weights add up to', sum(weights), '!= 1'
    return interval

def gen_weight_file():
    correct = False
    while not correct:
        ofile = raw_input('Enter output file name (' + DEFAULT_OUTFILE + '): ')
        if ofile.strip() == '':
            ofile = DEFAULT_OUTFILE
        print 'You entered', ofile
        correct = _confirm()

    portfolio = {}
    interval = 0
    keep_going = True
    while keep_going:
        portfolio[interval] = _gen_interval(interval)
        keep_going = _ask('Another interval (Y/n)? ')
        interval += 1
    
    portfolio_correct = False
    while not portfolio_correct:
        print '\nThe portfolio is as follows:\n '
        for interval in range(0,len(portfolio)):
            print 'Interval', interval, 'consists of:'
            print '\n'.join(portfolio[interval]) + '\n'

        portfolio_correct = _ask('Is this ok (Y/n)? ')
        if not portfolio_correct:
            print 'Oh my, I\'m terribly sorry to hear that!'
            correct = False
            while not correct:
                try:
                    interfix = int(_get_number('Enter the interval number'
                                            + ' you would like to fix: '))
                    if interfix in portfolio:
                        print 'You want to fix interval', interfix
                        correct = _confirm()
                        if correct:                        
                            print 'Ok, let\'s redo interval', interfix
                            portfolio[interfix] = _gen_interval(interfix)            
                    else:
                        print ('Interval ' + str(interfix) 
                               + ' does not exist!\nTry again.')
                except KeyboardInterrupt:
                    break

    with open(ofile, 'w') as f:
        for interval in range(0, len(portfolio)):
            f.write('\n'.join(portfolio[interval]) + '\n')
########################################################################
# END function definitions
########################################################################

if __name__ == "__main__":
    gen_weight_file()
