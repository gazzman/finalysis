CREATE FUNCTION fund_research.oex_components()
RETURNS TABLE(ticker VARCHAR(21),
              name VARCHAR,
              url VARCHAR)
AS $$
      SELECT ticker,
             name,
             url
      FROM oex_components
      WHERE date = (SELECT max(date)
                    FROM oex_components)
      ORDER BY ticker;
   $$
LANGUAGE SQL;
