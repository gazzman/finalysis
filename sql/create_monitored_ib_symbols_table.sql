CREATE TABLE analysis.monitored_ib_symbols (symbol VARCHAR(21), 
                                            trades bool,
                                            midpoint bool,
                                            bid bool,
                                            ask bool,
                                            bid_ask bool,
                                            historical_volatility bool,
                                            option_implied_volatility bool,
                                            PRIMARY KEY (symbol));
