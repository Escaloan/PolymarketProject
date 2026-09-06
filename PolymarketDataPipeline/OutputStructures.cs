// File for output structures to be stored in a database for the Polymarket data pipeline
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolymarketDataPipeline
{

    // Output structure for the Polymarket data pipeline
    public struct Event
    {
        // Event Information
        public Guid internal_id; // Internal ID for the event
        public int event_id;    // Polymarket Event ID
        public string slug;
        public string title;
        public long start_time;
        public long end_time;
        public bool five_minute;
        public bool fifteen_minute;

        public bool is_btc; // True if the trade is in BTC
        public bool is_eth; // True if the trade is in ETH
        public bool is_sol; // True if the trade is in SOL
        public bool is_xrp; // True if the trade is in XRP

        // Market Information
        public int market_id;
        public string condition_id;
        public float last_trade_price;
        public float up_outcome_price;
        public float down_outcome_price;
    }

    public struct User
    {
        public Guid internal_id; // Internal ID for the user
        public string proxy_wallet;
    }

    public struct Trade
    {
        // Internal Information
        public Guid internal_id; // Internal ID for the trade
        public Guid internal_event_id;
        public Guid internal_user_id;

        // Trade Information
        public float asset_price;   // Relative to the asset outcome, not position
        public float asset_size;
        public bool asset_outcome; // True for up, false for down

        public bool is_buy;
        public bool is_sell;    // If both are true, price and outcome are relative to the buy side of the trade event

        public bool opened_position; // Did the trade open a position or close.
                                     // close opens are counted as closes.

        public long trade_timestamp; // Taken from first event in Trade
        public long time_before_resolution; // Time before the event resolves, in seconds

        public bool is_btc; // True if the trade is in BTC
        public bool is_eth; // True if the trade is in ETH
        public bool is_sol; // True if the trade is in SOL
        public bool is_xrp; // True if the trade is in XRP

        public bool is_market_maker;
        public bool is_market_taker;

        public bool five_minute_trade; // True if the trade is part of a 5 minute market
        public bool fifteen_minute_trade; // True if the trade is part of a 15 minute market

        public int transaction_count; // Number of transactions in the trade
    }

    public struct Position 
    {
        // Internal Information
        public Guid internal_id; // Internal ID for the position
        public Guid internal_event_id;
        public Guid internal_user_id;

        // Position Information
        public float position_size; // Size of the position in shares
        public float entry_price; // Price at which the position was entered relative to the asset held
        public float exit_price; // Price at which the position was exited relative to the asset held

        public bool five_minute_market; // True if the position is part of a 5 minute market
        public bool fifteen_minute_market; // True if the position is part of a 5 minute market
        public bool asset_outcome; // True for up, false for down

        // Trade Dynamics Information
        public bool made_via_buy;
        public bool made_via_split;

        public bool exit_via_sell;
        public bool exit_via_merge;
        public bool exit_via_redeem;

        // Taker/Maker Information
        public bool entry_maker;
        public bool entry_taker;
        public bool exit_maker;
        public bool exit_taker;

        // Coin Information
        public bool is_btc; // True if the position is in BTC
        public bool is_eth; // True if the position is in ETH
        public bool is_sol; // True if the position is in SOL
        public bool is_xrp; // True if the position is in XRP

        // Time Information

        public int entry_before_resolution; // Time before the event resolves when the position was entered, in seconds
        public int exit_before_resolution; // Time before the event resolves when the position was exited, in seconds
                                           // 0 for positions that were redeemed
        public long exit_timestamp; // Timestamp of the last trade that closed the position
    }
}
