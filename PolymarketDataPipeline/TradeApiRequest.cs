using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolymarketDataPipeline
{
    internal class TradeApiRequest
    {
        private static readonly HttpClient client = new HttpClient()
        {
            Timeout = TimeSpan.FromSeconds(30)
        };

        // Event Api Info
        static string GammaEventUrl = "https://gamma-api.polymarket.com/events/keyset";
        static int EventPageLimit = 500;
        static int RateLimitDelay = 500; // Delay in milliseconds to avoid hitting rate limits

        // Target Event Info Parameters
        static string StartDataTime = "2026-05-01T04:00:01Z";
        static string EndDataTime = "2026-09-01T04:00:00Z";
        static int[] requestedSeriesId = [
            10684,  // - BTC  5m
            10192,  // - BTC 15m
            10683,  // - ETH  5m
            10191,  // - ETH 15m
            10686,  // - SOL  5m
            10423,  // - SOL 15m
            10685,  // - XRP  5m
            10422   // - XRP 15m 
        ];

        // Launch the worker threads to get events for all requested series
        public static async Task<List<Event>> GetEvents()
        {
            var tasks = requestedSeriesId
                .Select(seriesId => GetEventsWorker(seriesId));

            var results = await Task.WhenAll(tasks);

            return results
                .SelectMany(events => events)
                .ToList();
        }

        // Worker thread at works at a series
        public static async Task<List<Event>> GetEventsWorker(int seriesId)
        {
            List<Event> events = new List<Event>();

            string nextCursor = null;
            string baseUrl = $"{GammaEventUrl}?series_id={seriesId}&end_date_min={StartDataTime}&end_date_max={EndDataTime}&limit={EventPageLimit}";

            do
            {
                string url = nextCursor == null ? baseUrl : $"{baseUrl}&after_cursor={nextCursor}";
                var response = await client.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var (newEvents, newCursor) = TradeCompiler.JsonToEvent(response.ToString());
                    events.AddRange(newEvents);
                    nextCursor = newCursor;
                }
                else
                {
                    Console.WriteLine($"Error fetching events for series_id {seriesId}: {response.StatusCode}");
                }

                await Task.Delay(RateLimitDelay); // Delay to avoid hitting rate limits

            }
            while(nextCursor != null && nextCursor != "");

            return events;
        }
    }
}
