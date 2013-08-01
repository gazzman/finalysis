CREATE FUNCTION analysis.portfolio_benchmark_bars(pname text,
                                                  bmark text,
                                                  barsize integer, 
                                                  from_date date, 
                                                  to_date date)
RETURNS TABLE(barstart timestamp with time zone,
              p_open numeric(19,4), 
              p_high numeric(19,4), 
              p_low numeric(19,4), 
              p_close numeric(19,4),
              p_return numeric,
              p_rs_vol numeric,
              b_open numeric(19,4), 
              b_high numeric(19,4), 
              b_low numeric(19,4), 
              b_close numeric(19,4),
              b_return numeric,
              b_rs_vol numeric)
AS $$
      SELECT * 
      FROM (SELECT p.barstart, 
                   p.open,
                   max(p.high) OVER bar,
                   min(p.low) OVER bar,
                   last_value(p.close) OVER bar,
                   last_value(p.close) OVER bar/p.open - 1,
                   ln(max(p.high) OVER bar/last_value(p.close) OVER bar)
                       *ln(max(p.high) OVER bar/p.open)
                       +ln(min(p.low) OVER bar/last_value(p.close) OVER bar)
                       *ln(min(p.low) OVER bar/p.open),
                   b.open,
                   max(b.high) OVER bar,
                   min(b.low) OVER bar,
                   last_value(b.close) OVER bar,
                   last_value(b.close) OVER bar/b.open - 1,
                   ln(max(b.high) OVER bar/last_value(b.close) OVER bar)
                       *ln(max(b.high) OVER bar/b.open)
                       +ln(min(b.low) OVER bar/last_value(b.close) OVER bar)
                       *ln(min(b.low) OVER bar/b.open)
            FROM portfolio_trades_5_secs(pname, from_date, to_date) AS p,
                 symbol_trades_5_secs(bmark, from_date, to_date) AS b                 
            WHERE p.barstart = b.barstart
            WINDOW bar AS (ROWS BETWEEN CURRENT ROW AND barsize - 1 FOLLOWING))
      AS alltimes
      WHERE mod(cast(EXTRACT(EPOCH FROM barstart) AS integer), barsize*5) = 0;
   $$
LANGUAGE SQL;
