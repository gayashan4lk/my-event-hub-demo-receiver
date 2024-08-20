using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using System.Text;

var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("settings.json", optional: false, reloadOnChange: true)
                .Build();

var EVENT_HUB_CONNECTION_STRING = configuration["AppSettings:EVENT_HUB_CONNECTION_STRING"];
var EVENT_HUB_INSTANCE_NAME = configuration["AppSettings:EVENT_HUB_INSTANCE_NAME"];
var STORAGE_CONNECTION_STRING = configuration["AppSettings:STORAGE_CONNECTION_STRING"];
var BLOB_CONTAINER_NAME = configuration["AppSettings:BLOB_CONTAINER_NAME"];

Console.WriteLine($"Event hub instance: {EVENT_HUB_INSTANCE_NAME}");
Console.WriteLine($"Blob container: {BLOB_CONTAINER_NAME}");

BlobContainerClient storageClient = new BlobContainerClient(STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME);

var processor = new EventProcessorClient(
    storageClient, 
    EventHubConsumerClient.DefaultConsumerGroupName, 
    EVENT_HUB_CONNECTION_STRING, 
    EVENT_HUB_INSTANCE_NAME);

processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

await processor.StartProcessingAsync();

await Task.Delay(TimeSpan.FromSeconds(30));

await processor.StopProcessingAsync();

Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
    return Task.CompletedTask;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventErrorArgs)
{
    Console.WriteLine($"\tPartition '{ eventErrorArgs.PartitionId }': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventErrorArgs.Exception.Message);
    return Task.CompletedTask;
}

//await using (var consumerClient = new EventHubConsumerClient(
//    EventHubConsumerClient.DefaultConsumerGroupName, 
//    EVENT_HUB_CONNECTION_STRING, 
//    EVENT_HUB_INSTANCE_NAME))
//{
//    Console.WriteLine("Listening for events...");
//    await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsAsync())
//    {
//        string data = Encoding.UTF8.GetString(partitionEvent.Data.EventBody.ToArray());
//        Console.WriteLine($"Message received. PartitionID: {partitionEvent.Partition.PartitionId}, PartitionKey:{partitionEvent.Data.PartitionKey}, Data: {data}");
//    }
//}