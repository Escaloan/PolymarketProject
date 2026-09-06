using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PolymarketDataPipeline
{

    public struct TradeJson
    {
        public string proxyWallet;
        public string side; // "BUY" or "SELL"
        public string conditionId;
        public float size;
        public float price;
        public long timestamp;
        public string outcome; // "Up" or "Down"
        public string transactionHash;
        public bool taker; // Maker if false
    }

    internal class TradeCompiler
    {
        // Hash table with all the proxywallet-Guid pairs
        public static ConcurrentDictionary<string, Guid> internalUserIds = new ConcurrentDictionary<string, Guid>();

        // Parse Json with filtering
        public static List<TradeJson> JsontoTradeJson(string json)
        {
            // Deserialize as a list, not a single object
            List<TradeJson> trades = JsonSerializer.Deserialize<List<TradeJson>>(json) ?? new List<TradeJson>();
            return trades;
        }

        public static List<TradeJson> RemoveDuplicate(List<TradeJson> trades)
        {
            if (trades.Count == TradeApiRequest.TradePageSize)
            {
                long finalTimestamp = trades.Last().timestamp;
                return trades.Where(t => t.timestamp < finalTimestamp).ToList();
            }
            return trades;
        }

        // Adds the Maker and Taker Info from the trades position in the Transaction Hash
        public static List<TradeJson> AddMakerTakerData(List<TradeJson> trades)
        {
            string lastHash = "";
            for (int i = 0; i < trades.Count; i++)
            {
                var trade = trades[i];

                if (trade.transactionHash != lastHash)
                {
                    trade.taker = true;
                    lastHash = trade.transactionHash;
                }
                else
                {
                    trade.taker = false;
                }

                trades[i] = trade; // Reassign the modified struct back to the list
            }

            return trades;
        }

        public static List<Trade> ParseEventTrades(List<TradeJson> trades, Event ev)
        {
            List<Trade> currentTrades = new List<Trade>();

            if (trades == null || trades.Count == 0)
            {
                return currentTrades;
                Console.WriteLine($"Empty Event {ev.slug}");
            }

            var sortedTrades = trades
                .OrderBy(t => t.proxyWallet)
                .ThenBy(t => t.timestamp)
                .ToList();

            List<TradeJson> currentUser = new List<TradeJson>();

            string currentWallet = "";
            foreach (var trade in sortedTrades) {
                if (trade.proxyWallet != currentWallet) {
                    currentTrades.AddRange(ParseUserTrades(currentUser, ev));
                    currentUser.Clear();
                    currentWallet = trade.proxyWallet;
                }
                currentUser.Add(trade);
            }
            currentTrades.AddRange(ParseUserTrades(currentUser, ev)); // Clean up last user

            return currentTrades;
        }

        // Creates the Trades from the raw trade data
        public static List<Trade> ParseUserTrades(List<TradeJson> trades, Event ev)
        {
            var result = new List<Trade>();

            if (trades == null || trades.Count == 0)
                return result;

            Guid userId = internalUserIds.GetOrAdd(
                trades[0].proxyWallet, _ => Guid.NewGuid());

            // Positive = net Up holdings; negative = net Down holdings.
            decimal netHolding = 0m;

            int i = 0;
            while (i < trades.Count)
            {
                TradeJson first = trades[i];

                bool firstIsBuy = first.side == "BUY";
                bool firstIsUp = first.outcome == "Up";
                bool movesUp = firstIsBuy == firstIsUp;

                // Compare prices relative to the outcome being effectively bought.
                decimal firstPrice = firstIsBuy
                    ? (decimal)first.price
                    : 1m - (decimal)first.price;

                decimal minPrice = firstPrice;
                decimal maxPrice = firstPrice;

                decimal totalSize = 0m;
                decimal buySize = 0m;
                decimal sellSize = 0m;
                decimal buyValue = 0m;
                decimal sellValue = 0m;

                bool hasBuy = false;
                bool hasSell = false;
                bool hasMaker = false;
                bool hasTaker = false;

                int j = i;
                while (j < trades.Count)
                {
                    TradeJson raw = trades[j];

                    bool isBuy = raw.side == "BUY";
                    bool isUp = raw.outcome == "Up";

                    decimal price = isBuy
                        ? (decimal)raw.price
                        : 1m - (decimal)raw.price;

                    decimal nextMin = Math.Min(minPrice, price);
                    decimal nextMax = Math.Max(maxPrice, price);

                    if ((isBuy == isUp) != movesUp ||
                        raw.timestamp - first.timestamp > 15 ||
                        nextMax - nextMin > 0.02m)
                    {
                        break;
                    }

                    minPrice = nextMin;
                    maxPrice = nextMax;

                    decimal size = (decimal)raw.size;
                    totalSize += size;

                    if (isBuy)
                    {
                        hasBuy = true;
                        buySize += size;
                        buyValue += size * (decimal)raw.price;
                    }
                    else
                    {
                        hasSell = true;
                        sellSize += size;
                        sellValue += size * (decimal)raw.price;
                    }

                    hasMaker |= !raw.taker;
                    hasTaker |= raw.taker;

                    j++;
                }

                // Reducing or crossing an existing position counts as closing.
                bool openedPosition =
                    netHolding == 0m ||
                    (netHolding > 0m && movesUp) ||
                    (netHolding < 0m && !movesUp);

                decimal priceSize = hasBuy ? buySize : sellSize;
                decimal priceValue = hasBuy ? buyValue : sellValue;

                result.Add(new Trade
                {
                    internal_id = Guid.NewGuid(),
                    internal_event_id = ev.internal_id,
                    internal_user_id = userId,

                    asset_price = priceSize > 0m
                        ? (float)(priceValue / priceSize)
                        : 0f,
                    asset_size = (float)totalSize,
                    asset_outcome = hasBuy ? movesUp : !movesUp,

                    is_buy = hasBuy,
                    is_sell = hasSell,
                    opened_position = openedPosition,

                    trade_timestamp = first.timestamp,
                    time_before_resolution = ev.end_time - first.timestamp,

                    is_btc = ev.is_btc,
                    is_eth = ev.is_eth,
                    is_sol = ev.is_sol,
                    is_xrp = ev.is_xrp,

                    is_market_maker = hasMaker,
                    is_market_taker = hasTaker,

                    five_minute_trade = ev.five_minute,
                    fifteen_minute_trade = ev.fifteen_minute,

                    transaction_count = j - i
                });

                netHolding += movesUp ? totalSize : -totalSize;
                i = j;
            }

            return result;
        }
    }
}
