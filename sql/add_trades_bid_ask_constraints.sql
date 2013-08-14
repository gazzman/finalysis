ALTER TABLE trades_1_day ADD CONSTRAINT trades_1_day_symbol_update
                             FOREIGN KEY (symbol) 
                             REFERENCES monitored_ib_symbols (symbol) 
                             ON UPDATE CASCADE;

ALTER TABLE trades_1_day ADD CONSTRAINT trades_1_day_symbol_delete
                             FOREIGN KEY (symbol) 
                             REFERENCES monitored_ib_symbols (symbol) 
                             ON DELETE CASCADE;

ALTER TABLE trades_5_secs ADD CONSTRAINT trades_5_secs_symbol_update
                              FOREIGN KEY (symbol) 
                              REFERENCES monitored_ib_symbols (symbol) 
                              ON UPDATE CASCADE;

ALTER TABLE trades_5_secs ADD CONSTRAINT trades_5_secs_symbol_delete
                              FOREIGN KEY (symbol) 
                              REFERENCES monitored_ib_symbols (symbol) 
                              ON DELETE CASCADE;

ALTER TABLE bid_ask_1_day ADD CONSTRAINT bid_ask_1_day_symbol_update
                              FOREIGN KEY (symbol) 
                              REFERENCES monitored_ib_symbols (symbol) 
                              ON UPDATE CASCADE;

ALTER TABLE bid_ask_1_day ADD CONSTRAINT bid_ask_1_day_symbol_delete
                              FOREIGN KEY (symbol) 
                              REFERENCES monitored_ib_symbols (symbol) 
                              ON DELETE CASCADE;

ALTER TABLE bid_ask_5_secs ADD CONSTRAINT bid_ask_5_secs_symbol_update
                               FOREIGN KEY (symbol) 
                               REFERENCES monitored_ib_symbols (symbol) 
                               ON UPDATE CASCADE;

ALTER TABLE bid_ask_5_secs ADD CONSTRAINT bid_ask_5_secs_symbol_delete
                               FOREIGN KEY (symbol) 
                               REFERENCES monitored_ib_symbols (symbol) 
                               ON DELETE CASCADE;
