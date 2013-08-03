CREATE FUNCTION fund_research.spx_components()
RETURNS TABLE(ticker VARCHAR(21),
              name VARCHAR,
              url VARCHAR)
AS $$
      SELECT ticker,
             name,
             url
      FROM spx_components
      WHERE date = (SELECT max(date)
                    FROM spx_components)
      ORDER BY ticker;
   $$
LANGUAGE SQL;
