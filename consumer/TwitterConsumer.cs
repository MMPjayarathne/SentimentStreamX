using System;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace consumer
{
    public class TwitterConsumer
    {
        public static async Task RunConsumerAsync()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "twitter-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe("twitter-data");

            Console.WriteLine("Consuming tweets...");

            while (true)
            {
                var result = consumer.Consume();
                Console.WriteLine($"Got Something:{result}");
                var tweet = JObject.Parse(result.Value);

                string text = tweet["Text"]?.ToString();
                int sentiment = GetSentimentScore(text);

                Console.WriteLine($"Tweet: {text}");
                Console.WriteLine($"Sentiment Score: {sentiment}");
            }
        }

        static int GetSentimentScore(string text)
        {
            return text.Split(' ').Length > 5 ? 1 : -1;
        }
    }
}
