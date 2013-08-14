CREATE FUNCTION analysis.symbol_bid_ask_5_secs(symbol text, 
                                              from_date date, 
                                              to_date date)
RETURNS TABLE(barstart timestamp with time zone,
              open numeric(19,4), 
              high numeric(19,4), 
              low numeric(19,4), 
              close numeric(19,4))
AS $$ 
      SELECT timestamp AS barstart, 
             open,
             high,
             low,
             close
      FROM bid_ask_5_secs
      WHERE symbol = symbol_bid_ask_5_secs.symbol
      AND date(timestamp) >= from_date
      AND date(timestamp) <= to_date
      ORDER BY timestamp;
   $$
LANGUAGE SQL;
