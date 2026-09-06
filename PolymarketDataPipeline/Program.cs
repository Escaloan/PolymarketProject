using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolymarketDataPipeline
{
    internal class Program
    {
        static string basePath = @"D:\Code\PolymarketData";
        static string eventPath = $@"{basePath}\EventData.parquet";
        static string userPath = $@"{basePath}\UserData.parquet";
        static string tradePath = $@"{basePath}\TradeData.parquet";
        static string positionPath = $@"{basePath}\PositionData.parquet";

        public static string StartDataTime = "2026-05-01T04:00:01Z";
        public static string EndDataTime = "2026-09-01T04:00:00Z";
        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Phase 1 ... Collecting Events");
            var events = await EventApiRequest.GetEvents();
            bool hasDuplicates = events.GroupBy(e => e.internal_id).Any(g => g.Count() > 1);

            if(hasDuplicates)
            {
                throw new Exception("Duplicate UUID created for Events");
            }

            Console.WriteLine($"Fetched {events.Count} events\n");

            Console.WriteLine($"Saving Events to {eventPath} ...");
            await ParquetWriters.EventWriterParquet(events, eventPath);
            Console.WriteLine("Finish saving events\n");


            Console.WriteLine("Starting Phase 2 ... Collecting Trades and Processing Positions");

            // Starting Parquet Writers
            Task TradeWriter = ParquetWriters.TradeWriterParquet(TradeApiRequest.ProcessedTrades.Reader, tradePath);
            Task PositionWriter = ParquetWriters.PositionWriterParquet(TradeApiRequest.ProcessedPositions.Reader, positionPath);

            // Starting Trade Request Manager that will start the worker threads
            await TradeApiRequest.TradeThreadManager(events);

            await Task.WhenAll([TradeWriter, PositionWriter]);

            Console.WriteLine("Finished collecting trades\n");

            Console.WriteLine("Working on saving data ...");
            await ParquetWriters.UserWriterParquet(TradeCompiler.internalUserIds, userPath);
            ParquetWriters.SortPositionsParquet(positionPath, 4096);
            Console.WriteLine("Finished Saving Data. Polymarket Data Pipeline Complete");


        }
    }
}

