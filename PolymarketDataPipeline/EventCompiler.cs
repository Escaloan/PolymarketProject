using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PolymarketDataPipeline
{
    public record EventJsonWrapper
    (
        string next_cursor,
        List<EventJson> events
    );

    public record EventJson
    (
        string id,
        string slug,
        string title,
        string endDate, 
        List<MarketJson> markets
    );

    public record MarketJson
    (
        string id,
        string conditionId,
        float lastTradePrice,
        string outcomePrices
    );

    public class EventCompiler
    {
        public static (List<Event> Events, string nextCursor) JsonToEvent(string json)
        {
            EventJsonWrapper eventWrapper = JsonParseEvent(json);
            return (
                ParsedJsonToEvents(eventWrapper), 
                eventWrapper.next_cursor != null ? eventWrapper.next_cursor : ""
                );
        }

        // Parses Json into a Record
        public static EventJsonWrapper JsonParseEvent(string json)
        {
            try
            {
                return JsonSerializer.Deserialize<EventJsonWrapper>(json);
            }
            catch (JsonException ex)
            {
                throw new Exception($"Error parsing JSON: {ex.Message}, JSON: {json}");
            }
        }

        public static List<Event> ParsedJsonToEvents(EventJsonWrapper eventWrapper)
        {
            List<Event> events = new List<Event>();
            foreach (var eventJson in eventWrapper.events)
            {
                events.Add(ConvertJsonRecordToEvent(eventJson));
            }
            return events;
        }

        public static Event ConvertJsonRecordToEvent(EventJson eventJson)
        {
            try
            {
                if(eventJson.markets == null || eventJson.markets.Count == 0)
                {
                    throw new Exception($"Event {eventJson.slug} has no markets.");
                }

                string[] outcomeValues = JsonSerializer.Deserialize<string[]>(eventJson.markets[0].outcomePrices);

                if (outcomeValues.Length != 2)
                {
                    throw new Exception($"Event {eventJson.slug} has an bad outcome prices.");
                }

                Event ev = new Event
                {
                    internal_id = Guid.NewGuid(),
                    event_id = int.Parse(eventJson.id),
                    slug = eventJson.slug,
                    title = eventJson.title,
                    start_time = long.Parse(eventJson.slug.Split('-').Last()),
                    end_time = DateTimeOffset.Parse(eventJson.endDate).ToUnixTimeSeconds(),
                    five_minute = eventJson.slug.Contains("-5m"),
                    fifteen_minute = eventJson.slug.Contains("-15m"),
                    is_btc = eventJson.slug.Contains("btc"),
                    is_eth = eventJson.slug.Contains("eth"),
                    is_sol = eventJson.slug.Contains("sol"),
                    is_xrp = eventJson.slug.Contains("xrp"),
                    market_id = int.Parse(eventJson.markets[0].id),
                    condition_id = eventJson.markets[0].conditionId,
                    last_trade_price = eventJson.markets[0].lastTradePrice,
                    up_outcome_price = float.Parse(outcomeValues[0]),
                    down_outcome_price = float.Parse(outcomeValues[1])
                };

                return ev;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error converting EventJson to Event for event {eventJson.slug}: {ex.Message}");
            }
        }
    }
}
