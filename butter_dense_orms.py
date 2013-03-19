#!/usr/bin/python
from sqlalchemy import Table
from sqlalchemy import Column, DateTime
from sqlalchemy.dialects.postgresql import NUMERIC, VARCHAR

def gen_table(tablename, metadata, schema=None):
    return Table(tablename, metadata,
                 Column('underlying', VARCHAR(21), index=True, 
                        primary_key=True),
                 Column('interval', NUMERIC(19,4), index=True, 
                        primary_key=True),
                 Column('timestamp', DateTime(timezone=True), index=True, 
                        primary_key=True),
                 Column('last_open', NUMERIC(19,4)),
                 Column('last_high', NUMERIC(19,4)),
                 Column('last_low', NUMERIC(19,4)),
                 Column('last_close', NUMERIC(19,4)),
                 Column('volume', NUMERIC(19,4)),
                 Column('wap', NUMERIC(19,4)),
                 Column('count', NUMERIC(19,4)),
                 Column('bid', NUMERIC(19,4)),
                 Column('ask', NUMERIC(19,4)),
                 Column('bc_l4_bid', NUMERIC(19,4)),
                 Column('bc_l4_ask', NUMERIC(19,4)),
                 Column('bc_l3_bid', NUMERIC(19,4)),
                 Column('bc_l3_ask', NUMERIC(19,4)),
                 Column('bc_l2_bid', NUMERIC(19,4)),
                 Column('bc_l2_ask', NUMERIC(19,4)),
                 Column('bc_l1_bid', NUMERIC(19,4)),
                 Column('bc_l1_ask', NUMERIC(19,4)),
                 Column('bc_r1_bid', NUMERIC(19,4)),
                 Column('bc_r1_ask', NUMERIC(19,4)),
                 Column('bc_r2_bid', NUMERIC(19,4)),
                 Column('bc_r2_ask', NUMERIC(19,4)),
                 Column('bc_r3_bid', NUMERIC(19,4)),
                 Column('bc_r3_ask', NUMERIC(19,4)),
                 Column('bc_r4_bid', NUMERIC(19,4)),
                 Column('bc_r4_ask', NUMERIC(19,4)),
                 Column('bp_l4_bid', NUMERIC(19,4)),
                 Column('bp_l4_ask', NUMERIC(19,4)),
                 Column('bp_l3_bid', NUMERIC(19,4)),
                 Column('bp_l3_ask', NUMERIC(19,4)),
                 Column('bp_l2_bid', NUMERIC(19,4)),
                 Column('bp_l2_ask', NUMERIC(19,4)),
                 Column('bp_l1_bid', NUMERIC(19,4)),
                 Column('bp_l1_ask', NUMERIC(19,4)),
                 Column('bp_r1_bid', NUMERIC(19,4)),
                 Column('bp_r1_ask', NUMERIC(19,4)),
                 Column('bp_r2_bid', NUMERIC(19,4)),
                 Column('bp_r2_ask', NUMERIC(19,4)),
                 Column('bp_r3_bid', NUMERIC(19,4)),
                 Column('bp_r3_ask', NUMERIC(19,4)),
                 Column('bp_r4_bid', NUMERIC(19,4)),
                 Column('bp_r4_ask', NUMERIC(19,4)),
                 schema=schema)
