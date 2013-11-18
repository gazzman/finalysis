CREATE FUNCTION fund_research.current_weeklys_by_index()
RETURNS TABLE(ticker VARCHAR(21), 
              alt_ticker VARCHAR(21), 
              product_type VARCHAR, 
              djx int, 
              oex int, 
              spx int)
AS $$
      SELECT DISTINCT ON (ticker) ticker, 
                                  alt_ticker, 
                                  product_type, 
                                  djx, 
                                  oex, 
                                  spx 
      FROM (SELECT ticker, 
                   alt_ticker, 
                   product_type, 
                   djx, 
                   oex, 
                   spx, 
                   indexed 
            FROM index_weeklys_components()
            UNION
            SELECT ticker, 
                   alt_ticker, 
                   product_type, 
                   0, 
                   0, 
                   0, 
                   0
            FROM current_weeklys())
      AS u
      ORDER BY ticker, 
               indexed DESC;
   $$
LANGUAGE SQL;
