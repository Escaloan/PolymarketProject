using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace PolymarketDataPipeline
{
    internal class TradeApiRequest
    {
        // Trade API Info
        static string TradeDataPath = "https://data-api.polymarket.com/trades";
        static bool takerOnly = false; // Must be included for full picture
        public static int TradePageSize = 5000;
        static int TradeRateLimit = 18;
        static int MaximumAttemps = 5;

        // Channels
        static Channel<Event> UnprocessedEvents = Channel.CreateUnbounded<Event>();
        public static Channel<List<Trade>> ProcessedTrades = Channel.CreateUnbounded<List<Trade>>();
        public static Channel<Position> ProcessedPositions = Channel.CreateUnbounded<Position>();

        // HTTP Client Info
        private static readonly HttpClient client = new HttpClient()
        {
            Timeout = TimeSpan.FromSeconds(30)
        };

        public static async Task TradeThreadManager(List<Event> events)
        {
            // Add Events to channel
            foreach (Event ev in events) {
                UnprocessedEvents.Writer.TryWrite(ev);
            }
            UnprocessedEvents.Writer.TryComplete();

            TradeThreadWatcher();   

            // Spawn Worker Thread
            var WorkerThreads = new List<Task>();
            for(int i =0; i < TradeRateLimit; i++)
            {
                WorkerThreads.Add(TradeWorker(UnprocessedEvents.Reader));
            }
            await Task.WhenAll(WorkerThreads);

            // Close the Event and Postion Channels
            ProcessedTrades.Writer.TryComplete();
            ProcessedPositions.Writer.TryComplete();
        }

        public static async Task TradeThreadWatcher()
        {
            int startingEvents = UnprocessedEvents.Reader.Count;
            long startingTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            while (UnprocessedEvents.Reader.Count > 0)
            {
                int currentQueueCount = UnprocessedEvents.Reader.Count;
                await Task.Delay(30_000);
                long secondsElapsed = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - startingTime;
                int EventsProcessed = startingEvents - currentQueueCount;
                float ETA = (float)currentQueueCount * (float)secondsElapsed / (float)EventsProcessed;
                Console.WriteLine($"Processed {EventsProcessed} / {startingEvents} in {secondsElapsed} seconds. ETA(min): {ETA / 60}");
            }
        }

        // Worker thread that request the Trade data from the API and sends it off for processing
        public static async Task TradeWorker(ChannelReader<Event> reader)
        {
            var taskList = new List<Task>();

            await foreach(Event ev in reader.ReadAllAsync())
            {
                // Get Raw Trades
                List<TradeJson> rawTrades = await GetTradesForEvent(ev);

                taskList.Add(ProcessTrades(rawTrades, ev));
            }

            // Await all processing task
            await Task.WhenAll(taskList);
        }

        // Seperate Function to process trades to avoid messing up TradeWorker's API time
        public static async Task ProcessTrades(List<TradeJson> rawTrades, Event ev)
        {
            // Process Trades
            List<Trade> trades = TradeCompiler.ParseEventTrades(rawTrades, ev);
            List<Position> positions = PositionCompiler.TradesToPositions(trades, ev);


            ProcessedTrades.Writer.TryWrite(trades.OrderBy(t => t.trade_timestamp).ToList()); // Sorts trades by Timestamp
            foreach (Position position in positions) {
                ProcessedPositions.Writer.TryWrite(position);
            }
        }

        // Trade requestion logic for an Event
        public static async Task<List<TradeJson>> GetTradesForEvent(Event ev)
        {
            string baseUrl = $"{TradeDataPath}?limit={TradePageSize}&takerOnly={takerOnly}&market={ev.condition_id}";
            long CurrentTimeCursor = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            List<TradeJson> rawTrades = new List<TradeJson>();
            int lastBatchSize = 0;
            int attemps = 0;
            do
            {
                var T = Task.Delay(1000); // Rate Limitor

                // Request Trade Data
                try
                {
                    string url = $"{baseUrl}&end={CurrentTimeCursor}";
                    using var response = await client.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        attemps = 0;
                        var tradeBatch = TradeCompiler.JsontoTradeJson(await response.Content.ReadAsStringAsync());
                        lastBatchSize = tradeBatch.Count;
                        if (lastBatchSize == TradePageSize)
                        {
                            CurrentTimeCursor = tradeBatch.Last().timestamp;
                        }

                        rawTrades.AddRange(TradeCompiler.RemoveDuplicate(tradeBatch));
                    }
                    else
                    {
                        // If failure, increase attempts and pause
                        attemps++;
                        lastBatchSize = TradePageSize;
                        await Task.Delay(10000);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in {ev.slug}, {ex.ToString()}");
                    attemps++;
                    lastBatchSize = TradePageSize;
                    await Task.Delay(10000);
                }

                await T;
            }
            while (lastBatchSize == TradePageSize && attemps < MaximumAttemps);

            // Clear data if there is an error
            if (attemps == MaximumAttemps)
            {
                Console.WriteLine($"Error, failed to get Trades for {ev.title}, {ev.condition_id}");
                return [];
            }

            return rawTrades;
        }

    }
}
