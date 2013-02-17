#!/usr/bin/python
# -*- coding: latin-1 -*-
fidelity_map = {'Account Name/Number': None,
                'Symbol': 'symbol',
                'Description': 'description',
                'Quantity': 'qty',
                'Most Recent Price': 'price',
                'Most Recent Change': None,
                'Most Recent Value': 'total_value',
                'Change Since Last Close $': None,
                'Change Since Last Close %': None,
                'Type': None,
                'Grant ID': None,
                'Grant Price': None,
                'Offering Period': None,
                'Total Balance': None}

schwab_map = {"Symbol": 'symbol',
              "Name": 'description',
              "Quantity": 'qty',
              "Price": 'price',
              "Change": None,
              "Market Value": 'total_value',
              "Day Change($)": None,
              "Day Change(%)": None,
              "Reinvest Dividends?": None,
              "Capital Gain": None,
              "% of Account": None,
              "Security Type": None}

scottrade_map = {'﻿Symbol': 'symbol',
                 '﻿﻿Symbol': 'symbol',
                 'Symbol': 'symbol',
                 'Description': 'description',
                 'Acct Type': None,
                 'Qty': 'qty',
                 'Last Price': 'price',
                 '$ Chg': None,
                 '% Chg': None,
                 'Mkt Value': 'total_value',
                 '12 Month ROR': None,
                 '20 Day Volatility': None,
                 '52-Wk High': None,
                 '52-Wk High Date': None,
                 '52-Wk Low': None,
                 '52-Wk Low Date': None,
                 'Annual Dividend': None,
                 'Ask': None,
                 'Ask Exchange': None,
                 'Ask Size': None,
                 'Average Daily Volume (100)': None,
                 'Average Daily Volume (22)': None,
                 'Beta': None,
                 'Bid': None,
                 'Bid Exchange': None,
                 'Bid Size': None,
                 'Cur Qtr Est EPS': None,
                 'Cur Year Est EPS': None,
                 'Currency': None,
                 'CUSIP': None,
                 'Div Pay Date': None,
                 'Dividend Yield': None,
                 'Est Report Date': None,
                 'Growth 5 Year': None,
                 'High': None,
                 'Last 12 Month EPS': None,
                 'Last Dividend': None,
                 'Last Ex-Div Date': None,
                 'Low': None,
                 'Month Close Price': None,
                 'Moving Average (100)': None,
                 'Moving Average (21)': None,
                 'Moving Average (50)': None,
                 'Moving Average (9)': None,
                 'NAV': None,
                 'Next Ex-Div Date': None,
                 'Next Qtr Est EPS': None,
                 'Next Year Est EPS': None,
                 'Open': None,
                 'Open Interest': None,
                 'P/E Ratio': None,
                 'Prev Close': None,
                 'Primary Exchange': None,
                 'Qtr Close Price': None,
                 'Time Traded': None,
                 'Total Chg $': None,
                 'Volume': None,
                 'Week Close Price': None,
                 'Year Close Price': None,
                 '% off 52-Wk Low': None,
                 '% off 52-Wk High': None}
