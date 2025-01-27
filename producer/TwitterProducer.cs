using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Tweetinvi;
using Tweetinvi.Streaming;

namespace producer
{
    public class TwitterProducer
    {
         public static async Task RunProducerAsync()
        {


            string apiKey = Environment.GetEnvironmentVariable("TWITTER_API_KEY");

            string apiSecret = Environment.GetEnvironmentVariable("TWITTER_API_SECRET"); 
            string accessToken = Environment.GetEnvironmentVariable("TWITTER_ACCESS_TOKEN"); 
            string accessTokenSecret = Environment.GetEnvironmentVariable("TWITTER_ACCESS_SECRET"); 

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            var client = new TwitterClient(apiKey, apiSecret, accessToken, accessTokenSecret);
            var stream = client.Streams.CreateFilteredStream();

            stream.AddTrack("Sri Lanka");
            stream.AddTrack("SriLankaTourism");
            stream.AddTrack("Sri Lanka vacation");
            stream.AddTrack("Sri Lanka travel");
            stream.AddTrack("visit Sri Lanka");
            stream.AddTrack("#SriLanka");

            using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

    
            stream.MatchingTweetReceived += async (sender, args) =>
            {
                var tweet = args.Tweet;
                var message = JsonSerializer.Serialize(new
                {
                    User = tweet.CreatedBy.ScreenName,
                    Text = tweet.Text,
                    CreatedAt = tweet.CreatedAt,
                    Likes = tweet.FavoriteCount,
                    Location = tweet.Place?.FullName ?? "Unknown"
                });


                await producer.ProduceAsync("tourism-twitter-data", new Message<Null, string> { Value = message });
                Console.WriteLine($"Produced: {message}");
            };


            Console.WriteLine("Streaming tweets about Sri Lanka tourism...");
            await stream.StartMatchingAnyConditionAsync();
        }
    }
}
