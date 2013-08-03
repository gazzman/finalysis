CREATE FUNCTION fund_research.current_equity()
RETURNS TABLE(ticker VARCHAR(21), 
              date date)
AS $$ 
      SELECT ticker, 
             max(date)
      FROM equity
      GROUP BY ticker
      ORDER BY ticker;
   $$
LANGUAGE SQL;
