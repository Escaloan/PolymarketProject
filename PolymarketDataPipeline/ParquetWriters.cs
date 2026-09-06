using DuckDB.NET.Data;
using Parquet;
using Parquet.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace PolymarketDataPipeline
{
    internal class ParquetWriters
    {
        public static async Task EventWriterParquet(List<Event> events, string EventPath)
        {
            ArgumentNullException.ThrowIfNull(events);
            ArgumentException.ThrowIfNullOrWhiteSpace(EventPath);

            string fullPath = Path.GetFullPath(EventPath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            // Store the GUID as text; preserve the other field types.
            var rows = events.Select(ev => new
            {
                internal_id = ev.internal_id.ToString("D"),
                ev.event_id,
                ev.slug,
                ev.title,
                ev.start_time,
                ev.end_time,
                ev.five_minute,
                ev.fifteen_minute,
                ev.is_btc,
                ev.is_eth,
                ev.is_sol,
                ev.is_xrp,
                ev.market_id,
                ev.condition_id,
                ev.last_trade_price,
                ev.up_outcome_price,
                ev.down_outcome_price
            });

            await using var stream = new FileStream(
                fullPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 65536,
                useAsync: true);

            await ParquetSerializer.SerializeAsync(rows, stream);
        }

        public static async Task UserWriterParquet(ConcurrentDictionary<string, Guid> UserMap, string userPath)
        {
            ArgumentNullException.ThrowIfNull(UserMap);
            ArgumentException.ThrowIfNullOrWhiteSpace(userPath);

            string fullPath = Path.GetFullPath(userPath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            var rows = UserMap.ToArray().Select(user => new
            {
                proxyWallet = user.Key,
                internal_user_id = user.Value.ToString("D")
            });

            await using var stream = new FileStream(
                fullPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 65536,
                useAsync: true);

            await ParquetSerializer.SerializeAsync(rows, stream);
        }

        public static async Task TradeWriterParquet(ChannelReader<List<Trade>> trades, string tradePath)
        {
            ArgumentNullException.ThrowIfNull(trades);
            ArgumentException.ThrowIfNullOrWhiteSpace(tradePath);

            string fullPath = Path.GetFullPath(tradePath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            // Appending Parquet data requires a readable, seekable stream.
            await using var stream = new FileStream(
                fullPath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 65536,
                useAsync: true);

            const int targetBatchSize = 50_000;

            var buffer = new List<Trade>(targetBatchSize);
            bool append = false;
            bool completed = false;

            while (!completed)
            {
                while (buffer.Count < targetBatchSize)
                {
                    if (!await trades.WaitToReadAsync())
                    {
                        completed = true;
                        break;
                    }

                    if (trades.TryRead(out var list))
                    {
                        // Always add the entire list, preserving its order.
                        buffer.AddRange(list);
                    }
                }

                // An empty channel still produces an empty file with its schema.
                if (buffer.Count == 0 && append)
                    break;

                var rows = buffer.Select(t => new
                {
                    internal_id = t.internal_id.ToString("D"),
                    internal_event_id = t.internal_event_id.ToString("D"),
                    internal_user_id = t.internal_user_id.ToString("D"),

                    t.asset_price,
                    t.asset_size,
                    t.asset_outcome,

                    t.is_buy,
                    t.is_sell,
                    t.opened_position,

                    t.trade_timestamp,
                    t.time_before_resolution,

                    t.is_btc,
                    t.is_eth,
                    t.is_sol,
                    t.is_xrp,

                    t.is_market_maker,
                    t.is_market_taker,

                    t.five_minute_trade,
                    t.fifteen_minute_trade,

                    t.transaction_count
                });

                stream.Position = 0;

                await ParquetSerializer.SerializeAsync(
                    rows,
                    stream,
                    new ParquetOptions { Append = append });

                append = true;
                buffer.Clear();
            }
        }

        public static async Task PositionWriterParquet(ChannelReader<Position> positions,string positionPath)
        {
            ArgumentNullException.ThrowIfNull(positions);
            ArgumentException.ThrowIfNullOrWhiteSpace(positionPath);

            string fullPath = Path.GetFullPath(positionPath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            await using var stream = new FileStream(
                fullPath,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 65536,
                useAsync: true);

            const int targetBatchSize = 50_000;

            var buffer = new List<Position>(targetBatchSize);
            bool append = false;
            bool completed = false;

            while (!completed)
            {
                while (buffer.Count < targetBatchSize)
                {
                    if (!await positions.WaitToReadAsync())
                    {
                        completed = true;
                        break;
                    }

                    while (buffer.Count < targetBatchSize &&
                           positions.TryRead(out var position))
                    {
                        buffer.Add(position);
                    }
                }

                // An empty channel still produces an empty file with its schema.
                if (buffer.Count == 0 && append)
                    break;

                var rows = buffer.Select(p => new
                {
                    internal_id = p.internal_id.ToString("D"),
                    internal_event_id = p.internal_event_id.ToString("D"),
                    internal_user_id = p.internal_user_id.ToString("D"),

                    p.position_size,
                    p.entry_price,
                    p.exit_price,

                    p.five_minute_market,
                    p.fifteen_minute_market,
                    p.asset_outcome,

                    p.made_via_buy,
                    p.made_via_split,

                    p.exit_via_sell,
                    p.exit_via_merge,
                    p.exit_via_redeem,

                    p.entry_maker,
                    p.entry_taker,
                    p.exit_maker,
                    p.exit_taker,

                    p.is_btc,
                    p.is_eth,
                    p.is_sol,
                    p.is_xrp,

                    p.entry_before_resolution,
                    p.exit_before_resolution,
                    p.exit_timestamp
                });

                stream.Position = 0;

                await ParquetSerializer.SerializeAsync(
                    rows,
                    stream,
                    new ParquetOptions { Append = append });

                append = true;
                buffer.Clear();
            }
        }

        public static void SortPositionsParquet(string positionPath, int memoryLimitMB = 4096)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(positionPath);

            if (memoryLimitMB <= 0)
                throw new ArgumentOutOfRangeException(nameof(memoryLimitMB));

            string fullPath = Path.GetFullPath(positionPath);

            if (!File.Exists(fullPath))
                throw new FileNotFoundException("Position file not found.", fullPath);

            string tempDirectory = Path.Combine(
                Path.GetDirectoryName(fullPath)!,
                $"position-sort-{Guid.NewGuid():N}");

            Directory.CreateDirectory(tempDirectory);

            string sortedPath = Path.Combine(tempDirectory, "sorted.parquet");
            string spillPath = Path.Combine(tempDirectory, "spill");

            try
            {
                // Escape apostrophes for SQL string literals.
                string inputSql = fullPath.Replace("'", "''");
                string outputSql = sortedPath.Replace("'", "''");
                string spillSql = spillPath.Replace("'", "''");

                using (var connection = new DuckDBConnection("Data Source=:memory:"))
                {
                    connection.Open();

                    using var command = connection.CreateCommand();

                    command.CommandText = $@"
                SET memory_limit = '{memoryLimitMB}MB';
                SET threads = 2;
                SET temp_directory = '{spillSql}';
                SET preserve_insertion_order = true;

                COPY (
                    SELECT *
                    FROM read_parquet('{inputSql}', hive_partitioning = false)
                    ORDER BY internal_user_id ASC, exit_timestamp ASC
                )
                TO '{outputSql}'
                (FORMAT PARQUET, COMPRESSION ZSTD);
            ";

                    command.ExecuteNonQuery();
                }

                // All database file handles are closed before replacing the source.
                File.Move(sortedPath, fullPath, overwrite: true);
            }
            finally
            {
                if (Directory.Exists(tempDirectory))
                    Directory.Delete(tempDirectory, recursive: true);
            }
        }
    }
}
