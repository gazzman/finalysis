CREATE FUNCTION fund_research.djx_components()
RETURNS TABLE(ticker VARCHAR(21),
              name VARCHAR,
              url VARCHAR)
AS $$
      SELECT ticker,
             name,
             url
      FROM djx_components
      WHERE date = (SELECT max(date)
                    FROM djx_components)
      ORDER BY ticker;
   $$
LANGUAGE SQL;
