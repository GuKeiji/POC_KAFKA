using Confluent.Kafka;
using Newtonsoft.Json;

namespace KafkaDotNet.API;
internal class DataInferenceService : BackgroundService
{
    private readonly ILogger<DataInferenceService> _logger;

    public DataInferenceService(ILogger<DataInferenceService> logger)
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

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

        consumer.Subscribe("test-topic");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                if (consumeResult == null)
                {
                    continue;
                }

                _logger.LogInformation($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.Offset}'");
            }
            catch (Exception)
            {
                // Ignore
            }
        }


        return Task.CompletedTask;
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