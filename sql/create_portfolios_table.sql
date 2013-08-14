CREATE TABLE analysis.portfolios 
             (name text,
              symbol VARCHAR(21),
              qty NUMERIC(19,4),
              PRIMARY KEY (name, symbol));

ALTER TABLE analysis.portfolios ADD CONSTRAINT portfolios_symbol_trades_update 
                                    FOREIGN KEY (symbol) 
                                    REFERENCES monitored_ib_trades (symbol) 
                                    ON UPDATE CASCADE;
                                            
ALTER TABLE analysis.portfolios ADD CONSTRAINT portfolios_symbol_trades_delete 
                                    FOREIGN KEY (symbol) 
                                    REFERENCES monitored_ib_trades (symbol) 
                                    ON DELETE CASCADE;

ALTER TABLE analysis.portfolios ADD CONSTRAINT portfolios_symbol_bid_ask_update 
                                    FOREIGN KEY (symbol) 
                                    REFERENCES monitored_ib_bid_ask (symbol) 
                                    ON UPDATE CASCADE;
                                            
ALTER TABLE analysis.portfolios ADD CONSTRAINT portfolios_symbol_bid_ask_delete 
                                    FOREIGN KEY (symbol) 
                                    REFERENCES monitored_ib_bid_ask (symbol) 
                                    ON DELETE CASCADE;
