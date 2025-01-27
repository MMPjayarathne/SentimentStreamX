using System;
using System.Threading.Tasks;
using producer;  
using consumer;  

class Program
{
    public static async Task Main(string[] args)
    {
        var producerTask = TwitterProducer.RunProducerAsync();  
        var consumerTask = TwitterConsumer.RunConsumerAsync();  


        await Task.WhenAll(producerTask, consumerTask);
    }
}
