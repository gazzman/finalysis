#!/usr/bin/python
import SocketServer
import sys
import threading
from time import sleep

from finalysis.data_collection import parse_option_chain as poc
from finalysis.pcp_analysis import analyze_pcp as apcp

RESULTFILE = 'pcp_mispricings.txt'
HFMT = '{date:^10}, {time:^13}, {ticker:^6}, {call_id:^21}, {put_id:^21}, '
HFMT += '{call:^8}, {strike:^8}, {stock:^8}, {put:^8}, {cash:^9}'
DFMT = '{date:%Y-%m-%d}, {time:%H:%M:%S%z}, {ticker:^6}, {call_id:^21}, '
DFMT += '{put_id:^21}, {call:>8.3f}, {strike:>8.3f}, {stock:>8.3f}, '
DFMT += '{put:>8.3f}, {cash:>8.3f}'

def report_ls(result, ticker, call_id, put_id, ls_cash_out):
    header = HFMT.format(date='DATE', time='TIME', ticker='TICKER', 
                        call_id='CALL ID', put_id='PUT ID', call='CALL ASK',
                        strike='STRIKE', stock='STK BID', put='PUT BID',
                        cash='CASH OUT')

    data = DFMT.format(date=result['stock_date'], time=result['stock_time'],
                       ticker=ticker, call_id=call_id, put_id=put_id, 
                       call=result['call_ask'],
                       strike=result['contract_strike'],
                       stock=result['stock_bid'], put=result['put_bid'], 
                       cash=ls_cash_out)
               
    with open(RESULTFILE, 'a') as f:
        f.write(header + "\n")
        f.write(data + "\n"*3)

def report_sl(result, ticker, call_id, put_id, sl_cash_out):
    header = HFMT.format(date='DATE', time='TIME', ticker='TICKER', 
                        call_id='CALL ID', put_id='PUT ID', call='CALL BID',
                        strike='STRIKE', stock='STK ASK', put='PUT ASK',
                        cash='CASH OUT')

    data = DFMT.format(date=result['stock_date'], time=result['stock_time'],
                       ticker=ticker, call_id=call_id, put_id=put_id, 
                       call=result['call_bid'], 
                       strike=result['contract_strike'],
                       stock=result['stock_ask'], put=result['put_ask'], 
                       cash=sl_cash_out)
               
    with open(RESULTFILE, 'a') as f:
        f.write(header + "\n")
        f.write(data + "\n"*2)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        e_filename = self.request.recv(1024).strip()
        poc.ChainParser(e_filename, DBNAME, DBHOST)
        filename = e_filename.split('/')[-1]
        ticker, dt = filename.split('.html')[0].split('_')
        date, time = dt.split('T')
        p = apcp.PCPAnalyzer(DBNAME, dbhost=DBHOST)
        results = p.session.execute(p.base_query().\
                                  filter(apcp.stock.ticker==ticker).\
                                  filter(apcp.stock.date==date).\
                                  filter(apcp.stock.time==time).\
                                  filter(apcp.call.bid>0, apcp.put.bid>0).\
                                  order_by(apcp.call_contract.id, 
                                           apcp.stock.date, 
                                           apcp.stock.time)).fetchall()

        for result in results:
            ls_cash_out, sl_cash_out = p.cash_out_today(result, LEND, BORR)
            if ls_cash_out < 0 or sl_cash_out < 0:
                call_id = p.gen_contract_id(result, 'C')
                put_id = p.gen_contract_id(result, 'P')
                if ls_cash_out < 0:
                    report_ls(result, ticker, call_id, put_id, ls_cash_out)
                if sl_cash_out < 0:
                    report_sl(result, ticker, call_id, put_id, sl_cash_out)

if __name__ == '__main__':
    HOST = sys.argv[1]
    PORT = int(sys.argv[2])
    DBNAME = sys.argv[3]
    DBHOST = sys.argv[4]
    LEND = float(sys.argv[5])
    BORR = float(sys.argv[6])
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    server.serve_forever()
