CREATE FUNCTION fund_research.djx_weeklys_components()
RETURNS TABLE(ticker VARCHAR(21), 
              alt_ticker VARCHAR(21), 
              type VARCHAR, 
              name VARCHAR, 
              url VARCHAR)
AS $$
      SELECT components.ticker, 
             weeklys.alt_ticker, 
             weeklys.type, 
             components.name, 
             components.url
      FROM djx_components() AS components, 
           current_weeklys() AS weeklys
      WHERE components.ticker = weeklys.ticker
      ORDER BY ticker;
   $$
LANGUAGE SQL;
