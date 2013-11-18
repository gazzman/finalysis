CREATE FUNCTION analysis.consecutive_weeklys()
RETURNS TABLE(ticker varchar(21), 
              name varchar, 
              product_type varchar, 
              list_date date, 
              expiry_0 date,
              expiry_1 date,
              expiry_2 date,
              expiry_3 date,
              expiry_4 date,
              expiry_5 date,
              expiry_6 date)
AS $$
      SELECT a.* 
      FROM available_weeklys AS a 
      INNER JOIN (SELECT ticker, max(list_date) AS list_date 
                  FROM available_weeklys 
                  GROUP BY ticker) AS m 
      ON a.ticker = m.ticker 
      AND a.list_date = m.list_date 
      WHERE expiry_0 IS NOT NULL 
      AND expiry_1 IS NOT NULL 
      AND expiry_2 IS NOT NULL 
      AND expiry_3 IS NOT NULL 
      AND expiry_4 IS NOT NULL 
      ORDER BY a.product_type DESC, a.ticker;
   $$
LANGUAGE SQL;
