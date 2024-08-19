using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Configuration;
using System.Text;

var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("settings.json", optional: false, reloadOnChange: true)
                .Build();

var EVENT_HUB_CONNECTION_STRING = configuration["AppSettings:EVENT_HUB_CONNECTION_STRING"];
var EVENT_HUB_INSTANCE_NAME = configuration["AppSettings:EVENT_HUB_INSTANCE_NAME"];

await using (var consumerClient = new EventHubConsumerClient(
    EventHubConsumerClient.DefaultConsumerGroupName, 
    EVENT_HUB_CONNECTION_STRING, 
    EVENT_HUB_INSTANCE_NAME))
{
    Console.WriteLine("Listening for events...");
    await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsAsync())
    {
        string data = Encoding.UTF8.GetString(partitionEvent.Data.EventBody.ToArray());
        Console.WriteLine($"Message received. Partition: '{partitionEvent.Partition.PartitionId}', Data: '{data}'");
    }
}