CREATE FUNCTION analysis.portfolio_trades_5_secs(name text, 
                                                from_date date, 
                                                to_date date)
RETURNS TABLE(barstart timestamp with time zone, 
              open numeric(19,4), 
              high numeric(19,4), 
              low numeric(19,4), 
              close numeric(19,4))
AS $$
      SELECT timestamp AS barstart, 
             sum(open * qty) AS open,
             sum(high * qty) AS high,
             sum(low * qty) AS low,
             sum(close * qty) AS close
      FROM trades_5_secs, 
           (SELECT *
            FROM analysis.portfolios) AS portfolio
      WHERE portfolio.name = portfolio_trades_5_secs.name
      AND trades_5_secs.symbol = portfolio.symbol
      AND date(timestamp) >= from_date
      AND date(timestamp) <= to_date
      GROUP BY barstart
      ORDER BY barstart;
   $$
LANGUAGE SQL;
