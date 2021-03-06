CREATE FUNCTION analysis.portfolio_bid_ask_benchmark_bid_ask(pname text,
                                                             bmark text,
                                                             barsize integer, 
                                                             from_date date, 
                                                             to_date date)
RETURNS TABLE(barstart timestamp with time zone, 
              p_open numeric(19,4), 
              p_high numeric(19,4), 
              p_low numeric(19,4), 
              p_close numeric(19,4), 
              p_open_index numeric, 
              p_high_index numeric, 
              p_low_index numeric, 
              p_close_index numeric, 
              p_return numeric, 
              p_rs_vol numeric, 
              b_open numeric(19,4), 
              b_high numeric(19,4), 
              b_low numeric(19,4), 
              b_close numeric(19,4), 
              b_open_index numeric, 
              b_high_index numeric, 
              b_low_index numeric, 
              b_close_index numeric, 
              b_return numeric, 
              b_rs_vol numeric)
AS $$
      SELECT barstart, 
             p_open, 
             p_high, 
             p_low, 
             p_close, 
             p_open/first_value(p_open) OVER s * 100, 
             p_high/first_value(p_open) OVER s * 100, 
             p_low/first_value(p_open) OVER s * 100, 
             p_close/first_value(p_open) OVER s * 100, 
             p_return, 
             p_rs_vol, 
             b_open, 
             b_high, 
             b_low, 
             b_close, 
             b_open/first_value(b_open) OVER s * 100, 
             b_high/first_value(b_open) OVER s * 100, 
             b_low/first_value(b_open) OVER s * 100, 
             b_close/first_value(b_open) OVER s * 100, 
             b_return, 
             b_rs_vol
      FROM (SELECT p.barstart AS barstart, 
                   p.open AS p_open,
                   max(p.high) OVER bar AS p_high,
                   min(p.low) OVER bar AS p_low,
                   last_value(p.close) OVER bar AS p_close,
                   b.open AS b_open,
                   max(b.high) OVER bar AS b_high,
                   min(b.low) OVER bar AS b_low,
                   last_value(b.close) OVER bar AS b_close,
                   last_value(p.close) OVER bar/p.open - 1 AS p_return,
                   ln(max(p.high) OVER bar/last_value(p.close) OVER bar)
                       *ln(max(p.high) OVER bar/p.open)
                       +ln(min(p.low) OVER bar/last_value(p.close) OVER bar)
                       *ln(min(p.low) OVER bar/p.open) AS p_rs_vol, 
                   last_value(b.close) OVER bar/b.open - 1 AS b_return, 
                   ln(max(b.high) OVER bar/last_value(b.close) OVER bar)
                       *ln(max(b.high) OVER bar/b.open)
                       +ln(min(b.low) OVER bar/last_value(b.close) OVER bar)
                       *ln(min(b.low) OVER bar/b.open) AS b_rs_vol
            FROM portfolio_bid_ask_5_secs(pname, from_date, to_date) AS p,
                 symbol_bid_ask_5_secs(bmark, from_date, to_date) AS b                 
            WHERE p.barstart = b.barstart
            WINDOW bar AS (ROWS BETWEEN CURRENT ROW AND barsize - 1 FOLLOWING))
      AS alltimes
      WHERE mod(cast(EXTRACT(EPOCH FROM barstart) AS integer), barsize*5) = 0
      WINDOW s AS (ROWS UNBOUNDED PRECEDING);
   $$
LANGUAGE SQL;
