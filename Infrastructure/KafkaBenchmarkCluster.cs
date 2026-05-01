using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;

namespace KafkaBenchmarks.Infrastructure;

internal sealed class KafkaBenchmarkCluster : IAsyncDisposable
{
    private readonly KafkaContainer _kafkaContainer = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.6.1")
        .Build();

    public string BootstrapServers => _kafkaContainer.GetBootstrapAddress();

    public async Task StartAsync()
    {
        await _kafkaContainer.StartAsync();
        await WaitUntilReadyAsync();
    }

    public async Task CreateTopicAsync(string topicName, int partitions = 1)
    {
        using var admin = BuildAdminClient();
        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = 1,
                }
            });
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
    }

    public async Task ProduceJsonAsync(string topicName, IEnumerable<string> payloads)
    {
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
        }).Build();

        foreach (var payload in payloads)
        {
            await producer.ProduceAsync(topicName, new Message<byte[], byte[]>
            {
                Value = Encoding.UTF8.GetBytes(payload),
            });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task ProduceJsonAsync(string topicName, int messageCount, Func<int, string> payloadFactory, int flushEvery = 10000)
    {
        Console.WriteLine($"Priming topic {topicName} with {messageCount:N0} records...");
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
        }).Build();

        for (var i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topicName, new Message<byte[], byte[]>
            {
                Value = Encoding.UTF8.GetBytes(payloadFactory(i)),
            });

            if ((i + 1) % flushEvery == 0)
            {
                producer.Flush(TimeSpan.FromSeconds(30));
                Console.WriteLine($"Primed {i + 1:N0}/{messageCount:N0} records into {topicName}");
            }
        }

        producer.Flush(TimeSpan.FromMinutes(2));
        Console.WriteLine($"Finished priming topic {topicName}");
    }

    public async ValueTask DisposeAsync()
    {
        await _kafkaContainer.DisposeAsync();
    }

    private IAdminClient BuildAdminClient()
    {
        return new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = BootstrapServers,
        }).Build();
    }

    private async Task WaitUntilReadyAsync()
    {
        using var admin = BuildAdminClient();
        var deadline = DateTime.UtcNow.AddMinutes(1);

        while (true)
        {
            try
            {
                _ = admin.GetMetadata(TimeSpan.FromSeconds(2));
                return;
            }
            catch
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
    }
}