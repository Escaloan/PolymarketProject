using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolymarketDataPipeline
{
    internal class PositionCompiler
    {
        public static List<Position> TradesToPositions(List<Trade> trades, Event ev)
        {
            var positions = new List<Position>();

            if (trades == null || trades.Count == 0)
                return positions;

            foreach (var user in trades.GroupBy(t => t.internal_user_id))
            {
                var userTrades = user
                    .OrderBy(t => t.trade_timestamp)
                    .ToList();

                var entries = new List<(
                    Trade trade,
                    decimal remaining,
                    bool outcome)>();

                int firstEntry = 0;

                // The final iteration redeems any unmatched entry shares.
                for (int i = 0; i <= userTrades.Count; i++)
                {
                    bool redeem = i == userTrades.Count;
                    Trade trade = redeem ? default(Trade) : userTrades[i];

                    decimal remaining = redeem ? 0m : (decimal)trade.asset_size;

                    // A sell moves holdings toward the opposite outcome.
                    // When both flags are true, the outcome is relative to the buy.
                    bool outcome = trade.is_buy
                        ? trade.asset_outcome
                        : !trade.asset_outcome;

                    while (firstEntry < entries.Count &&
                           (redeem ||
                            (remaining > 0m &&
                             entries[firstEntry].outcome != outcome)))
                    {
                        var entry = entries[firstEntry];

                        decimal matchedSize = redeem
                            ? entry.remaining
                            : Math.Min(entry.remaining, remaining);

                        // Sell-only entries acquire the opposite asset via a split.
                        float entryPrice = entry.trade.is_buy
                            ? entry.trade.asset_price
                            : 1f - entry.trade.asset_price;

                        float exitPrice;

                        if (redeem)
                        {
                            exitPrice = entry.outcome
                                ? ev.up_outcome_price
                                : ev.down_outcome_price;
                        }
                        else
                        {
                            // Buying the opposite asset closes via a merge.
                            exitPrice = trade.is_buy
                                ? 1f - trade.asset_price
                                : trade.asset_price;
                        }

                        positions.Add(new Position
                        {
                            internal_id = Guid.NewGuid(),
                            internal_event_id = ev.internal_id,
                            internal_user_id = user.Key,

                            position_size = (float)matchedSize,
                            entry_price = entryPrice,
                            exit_price = exitPrice,
                            asset_outcome = entry.outcome,

                            five_minute_market = ev.five_minute,
                            fifteen_minute_market = ev.fifteen_minute,

                            made_via_buy = entry.trade.is_buy,
                            made_via_split = entry.trade.is_sell,

                            exit_via_sell = !redeem && trade.is_sell,
                            exit_via_merge = !redeem && trade.is_buy,
                            exit_via_redeem = redeem,

                            entry_maker = entry.trade.is_market_maker,
                            entry_taker = entry.trade.is_market_taker,
                            exit_maker = !redeem && trade.is_market_maker,
                            exit_taker = !redeem && trade.is_market_taker,

                            is_btc = ev.is_btc,
                            is_eth = ev.is_eth,
                            is_sol = ev.is_sol,
                            is_xrp = ev.is_xrp,

                            entry_before_resolution =
                                (int)(ev.end_time - entry.trade.trade_timestamp),

                            exit_before_resolution = redeem
                                ? 0
                                : (int)(ev.end_time - trade.trade_timestamp),

                            exit_timestamp = redeem
                                ? ev.end_time
                                : trade.trade_timestamp
                        });

                        entry.remaining -= matchedSize;
                        entries[firstEntry] = entry;

                        if (entry.remaining == 0m)
                            firstEntry++;

                        if (!redeem)
                            remaining -= matchedSize;
                    }

                    // Includes any excess shares from a trade that crosses zero.
                    if (!redeem && remaining > 0m)
                        entries.Add((trade, remaining, outcome));
                }
            }

            return positions;
        }

    }
}
