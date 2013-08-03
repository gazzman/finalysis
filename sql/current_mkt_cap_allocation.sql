CREATE FUNCTION fund_research.current_mkt_cap_allocation()
RETURNS TABLE(ticker VARCHAR(21), 
              date date)
AS $$ 
      SELECT ticker, 
             max(date)
      FROM mkt_cap_allocation
      GROUP BY ticker
      ORDER BY ticker;
   $$
LANGUAGE SQL;
