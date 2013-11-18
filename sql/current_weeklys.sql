CREATE FUNCTION fund_research.current_weeklys()
RETURNS TABLE(ticker VARCHAR(21),
              alt_ticker VARCHAR(21),
              product_type VARCHAR)
AS $$
      SELECT DISTINCT ON (ticker) *
      FROM (SELECT DISTINCT substring(ticker FROM 1 FOR char_length(ticker)-1) AS ticker, 
                            ticker AS alt_ticker, 
                            product_type
            FROM available_weeklys
            WHERE ticker SIMILAR TO '%[0-9]'
            AND list_date = (SELECT max(list_date)
                             FROM available_weeklys)
            UNION
            SELECT DISTINCT ticker, 
                            NULL, 
                            product_type
            FROM available_weeklys
            WHERE ticker NOT SIMILAR TO '%[0-9]'
            AND list_date = (SELECT max(list_date)
                             FROM available_weeklys)
      ORDER BY ticker, alt_ticker ASC) AS u;
   $$
LANGUAGE SQL;
