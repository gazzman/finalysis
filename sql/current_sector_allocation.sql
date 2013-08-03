CREATE FUNCTION fund_research.current_sector_allocation()
RETURNS TABLE(ticker VARCHAR(21), 
              date date)
AS $$ 
      SELECT ticker, 
             max(date)
      FROM sector_allocation
      GROUP BY ticker
      ORDER BY ticker;
   $$
LANGUAGE SQL;
