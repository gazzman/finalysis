CREATE FUNCTION prices.current_yahoo_daily_prices()
RETURNS TABLE(ticker VARCHAR(21), 
              date date)
AS $$ 
      SELECT ticker, 
             max(date)
      FROM yahoo_daily_prices
      GROUP BY ticker
      ORDER BY ticker;
   $$
LANGUAGE SQL;
