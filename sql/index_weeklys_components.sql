CREATE FUNCTION fund_research.index_weeklys_components()
RETURNS TABLE(ticker VARCHAR(21), 
              alt_ticker VARCHAR(21), 
              product_type VARCHAR, 
              djx smallint, 
              oex smallint, 
              spx smallint, 
              indexed smallint)
AS $$
      SELECT ticker, 
             alt_ticker, 
             product_type, 
             cast(sum(djx) AS smallint), 
             cast(sum(oex) AS smallint), 
             cast(sum(spx) AS smallint), 
             cast(1 AS smallint)
      FROM (SELECT ticker, 
                   alt_ticker, 
                   product_type, 
                   1 AS djx, 
                   0 AS oex, 
                   0 AS spx 
            FROM djx_weeklys_components() 
            UNION 
            SELECT ticker, 
                   alt_ticker, 
                   product_type, 
                   0, 
                   1, 
                   0 
            FROM oex_weeklys_components() 
            UNION 
            SELECT ticker, 
                   alt_ticker, 
                   product_type, 
                   0, 
                   0, 
                   1 
            FROM spx_weeklys_components()) 
      AS uuu 
      GROUP BY ticker, alt_ticker, product_type
      ORDER BY ticker;
   $$
LANGUAGE SQL;
