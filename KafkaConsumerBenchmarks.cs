extern alias EventReaderApp;

using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using EventProcessor.Kafka;
using EventProcessor.Logging;
using EventProcessor.Monitoring;
using EventProcessor.Rules;
using EventProcessor.Session;
using EventReaderApp::EventReader.Kafka;
using EventReaderApp::EventReader.Logging;
using EventReaderApp::EventReader.Monitoring;
using KafkaBenchmarks.Infrastructure;
using KoreForge.Kafka.Configuration.Extensions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Consumer.Hosting;
using Event.Streaming.Processing.Monitoring;
using KoreForge.Time;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KafkaBenchmarks;

[SimpleJob(RuntimeMoniker.HostProcess, launchCount: 1, warmupCount: 0, iterationCount: 1, invocationCount: 1)]
[MemoryDiagnoser]
public class KafkaConsumerBenchmarks
{
    private const int PrimedRecordCount = 100000;
    private const int TopicPartitionCount = 1;

    private readonly KafkaBenchmarkCluster _cluster = new();
    private string _eventReaderTopic = string.Empty;
    private string _eventProcessorTopic = string.Empty;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await _cluster.StartAsync();

        _eventReaderTopic = $"bench-event-reader-{Guid.NewGuid():N}";
        _eventProcessorTopic = $"bench-event-processor-{Guid.NewGuid():N}";

        await _cluster.CreateTopicAsync(_eventReaderTopic, TopicPartitionCount);
        await _cluster.CreateTopicAsync(_eventProcessorTopic, TopicPartitionCount);

        await _cluster.ProduceJsonAsync(_eventReaderTopic, PrimedRecordCount, CreateEventReaderPayload);
        await _cluster.ProduceJsonAsync(_eventProcessorTopic, PrimedRecordCount, CreateEventProcessorPayload);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _cluster.DisposeAsync();
    }

    [Benchmark(Description = "EventReader consume primed topic", OperationsPerInvoke = PrimedRecordCount)]
    public async Task<int> EventReaderConsumePrimedTopic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        EventReaderApp::EventReader.Logging.GeneratedLoggingServiceCollectionExtensions.AddGeneratedLogging(services);
        services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton<EventReaderMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();
        services.AddScoped<EventReaderKafkaBatchProcessor>();
        services.AddKafkaConfiguration(BuildConfiguration(_eventReaderTopic, $"bench-reader-{Guid.NewGuid():N}"));

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var processor = scope.ServiceProvider.GetRequiredService<EventReaderKafkaBatchProcessor>();
        var factory = scope.ServiceProvider.GetRequiredService<IKafkaClientConfigFactory>();
        var metrics = scope.ServiceProvider.GetRequiredService<EventReaderMetricsAccumulator>();
        var loggerFactory = scope.ServiceProvider.GetRequiredService<ILoggerFactory>();

        await using var host = KafkaConsumerHost.Create()
            .UseKafkaConfigurationProfile("Default", factory)
            .UseLoggerFactory(loggerFactory)
            .UseProcessor(() => processor)
            .Build();

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        await host.StartAsync(cts.Token);
        await WaitUntilAsync(() => metrics.TotalProcessed == PrimedRecordCount, TimeSpan.FromMinutes(10), cts.Token);
        await host.StopAsync(cts.Token);

        return checked((int)metrics.TotalProcessed);
    }

    [Benchmark(Description = "EventProcessor consume primed topic", OperationsPerInvoke = PrimedRecordCount)]
    public async Task<int> EventProcessorConsumePrimedTopic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        EventProcessor.Logging.GeneratedLoggingServiceCollectionExtensions.AddGeneratedLogging(services);
        services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
        services.AddSingleton<FraudSessionStore>();
        services.AddSingleton<RuleEvaluator>(_ =>
        {
            var evaluator = new RuleEvaluator();
            evaluator.RefreshRules([new HighFrequencyRule(3), new HighAmountRule(1000m)]);
            return evaluator;
        });
        services.AddSingleton<EventProcessorMonitoringSnapshot>();
        services.AddSingleton<EventProcessorMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();
        services.AddScoped<EventProcessorKafkaBatchProcessor>();
        services.AddKafkaConfiguration(BuildConfiguration(_eventProcessorTopic, $"bench-processor-{Guid.NewGuid():N}"));

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var processor = scope.ServiceProvider.GetRequiredService<EventProcessorKafkaBatchProcessor>();
        var factory = scope.ServiceProvider.GetRequiredService<IKafkaClientConfigFactory>();
        var metrics = scope.ServiceProvider.GetRequiredService<EventProcessorMetricsAccumulator>();
        var loggerFactory = scope.ServiceProvider.GetRequiredService<ILoggerFactory>();

        await using var host = KafkaConsumerHost.Create()
            .UseKafkaConfigurationProfile("Default", factory)
            .UseLoggerFactory(loggerFactory)
            .UseProcessor(() => processor)
            .Build();

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
        await host.StartAsync(cts.Token);
        await WaitUntilAsync(() => metrics.TotalProcessed == PrimedRecordCount, TimeSpan.FromMinutes(10), cts.Token);
        await host.StopAsync(cts.Token);

        return checked((int)metrics.TotalProcessed);
    }

    private IConfiguration BuildConfiguration(string topic, string groupId)
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Clusters:Local:BootstrapServers"] = _cluster.BootstrapServers,
                ["Kafka:Profiles:Default:Type"] = "Consumer",
                ["Kafka:Profiles:Default:Cluster"] = "Local",
                ["Kafka:Profiles:Default:ExplicitGroupId"] = groupId,
                ["Kafka:Profiles:Default:Topics:0"] = topic,
                ["Kafka:Profiles:Default:ConfluentOptions:auto.offset.reset"] = "earliest",
                ["Kafka:Profiles:Default:ConfluentOptions:enable.auto.commit"] = "false",
                ["Kafka:Profiles:Default:ExtendedConsumer:ConsumerCount"] = "1",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchSize"] = "250",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchWaitMs"] = "100",
                ["Kafka:Profiles:Default:ExtendedConsumer:StartMode"] = "Earliest",
            })
            .Build();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Kafka benchmark did not consume the expected record count before timeout.");
            }

            await Task.Delay(100, cancellationToken);
        }
    }

    private static string CreateEventReaderPayload(int index)
    {
        return JsonSerializer.Serialize(new
        {
            eventType = "payment.created",
            eventId = $"evt-{index}",
            entityId = $"acct-{index % 100}",
            amount = 10 + index,
            customer = new { country = "GB", segment = index % 2 == 0 ? "retail" : "business" },
            items = new[] { new { sku = $"sku-{index % 250}", qty = 1 } },
        });
    }

    private static string CreateEventProcessorPayload(int index)
    {
        return JsonSerializer.Serialize(new
        {
            entityId = $"acct-{index % 100}",
            amount = 600 + (index % 500),
            country = "GB",
            merchant = new { category = index % 3 == 0 ? "electronics" : "grocery", name = $"merchant-{index % 1000}" },
            signals = new[] { "velocity", "amount" },
        });
    }
}
