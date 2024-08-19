using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("settings.json", optional: false, reloadOnChange: true)
                .Build();

var EVENT_HUB_CONNECTION_STRING = configuration["AppSettings:EVENT_HUB_CONNECTION_STRING"];
var EVENT_HUB_INSTANCE_NAME = configuration["AppSettings:EVENT_HUB_INSTANCE_NAME"];

Console.WriteLine(EVENT_HUB_CONNECTION_STRING);
Console.WriteLine(EVENT_HUB_INSTANCE_NAME);