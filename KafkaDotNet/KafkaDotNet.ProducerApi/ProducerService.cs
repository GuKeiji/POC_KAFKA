using Confluent.Kafka;
using Newtonsoft.Json;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaDotNet.ProducerApi;

public class ProducerService
{
    private readonly ILogger<ProducerService> _logger;

    public ProducerService(ILogger<ProducerService> logger)
    {
        _logger = logger;
    }

    public class DataStreamingController
    {
        public DataStreamingController()
        {
            ControllerName = "Termometro do forno";
            Random random = new Random();
            Temperature = random.Next(15, 50);
        }

        public string ControllerName { get; set; }
        public int Temperature { get; set; }
    }

    public async Task ProduceAsync(CancellationToken cancellationToken)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            AllowAutoCreateTopics = true,
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            while (true)
            {
                DataStreamingController controller = new DataStreamingController();
                Thread.Sleep(5000);
                var deliveryResult = await producer.ProduceAsync(topic: "test-topic",
                new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(controller)
                },
                cancellationToken);

                _logger.LogInformation($"Delivered message to {deliveryResult.Value}, Offset: {deliveryResult.Offset}");
            }
        }
        catch (ProduceException<Null, string> e)
        {
            _logger.LogError($"Delivery failed: {e.Error.Reason}");
        }

        producer.Flush(cancellationToken);
    }
}
