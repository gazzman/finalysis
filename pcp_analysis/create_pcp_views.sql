CREATE VIEW pcp_data AS
    SELECT
         stock.date, 
         stock.time, 
         stock.ticker, 
         stock.ask stock_ask, 
         stock.bid stock_bid, 
         call_contract.id call_id, 
         put_contract.id put_id, 
         call_contract.expiry,
         call_contract.strike, 
         call.ask call_ask, 
         call.bid call_bid, 
         put.ask put_ask, 
         put.bid put_bid
    FROM 
        underlying_prices stock, 
        option_prices put, 
        option_prices call, 
        option_contracts call_contract, 
        option_contracts put_contract
    WHERE call_contract.call_put = 'C' 
        AND put_contract.call_put = 'P'
        AND call_contract.strike = put_contract.strike 
        AND call_contract.expiry = put_contract.expiry
        AND call_contract.ticker = put_contract.ticker
        AND call_contract.ticker = stock.ticker 
        AND stock.date = call.date 
        AND call.date = put.date
        AND stock.time = call.time 
        AND call.time = put.time
        AND call_contract.id = call.id 
        AND put_contract.id = put.id
;
