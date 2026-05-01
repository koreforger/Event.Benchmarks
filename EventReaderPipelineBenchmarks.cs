extern alias EventReaderApp;

using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using EventReaderApp::EventReader.Kafka;
using EventReaderApp::Event.Streaming.Processing.Runtime;
using EventReaderApp::Event.Streaming.Processing.WorkStore;
using KoreForge.Jex;
using Newtonsoft.Json.Linq;

namespace KafkaBenchmarks;

[SimpleJob]
[MemoryDiagnoser]
public class EventReaderPipelineBenchmarks
{
    private static readonly JsonPathSelector[] OneSelector = [JsonPathSelector.Create("$.Action")];
    private static readonly JsonPathSelector[] FiveSelectors =
    [
        JsonPathSelector.Create("$.Action"),
        JsonPathSelector.Create("$.NedbankID"),
        JsonPathSelector.Create("$.correlationId"),
        JsonPathSelector.Create("$.source"),
        JsonPathSelector.Create("$.version"),
    ];

    private JsonFieldScanner _scanner = null!;
    private byte[] _smallPayloadUtf8 = null!;
    private byte[] _mediumPayloadUtf8 = null!;
    private byte[] _largePayloadUtf8 = null!;
    private FunctionMatcherIndex _smallIndex = null!;
    private FunctionMatcherIndex _largeIndex = null!;
    private Jex _jexCompiler = null!;
    private IJexProgram _jexProgram = null!;
    private JObject _jexInput = null!;
    private CompiledRuleSet _smallRuleSet = null!;
    private CompiledRuleSet _largeRuleSet = null!;

    private EventReaderRuntimeModel _pipelineModel = null!;
    private JsonFieldScanner _pipelineScanner = null!;
    private ClientIdentityResolver _pipelineIdentityResolver = null!;
    private ShardAssigner _pipelineShardAssigner = null!;
    private EventReaderWorkItemProcessor _pipelineProcessor = null!;
    private IEventReaderWorkStore _pipelineStore = null!;
    private string _pipelineStoreTempPath = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _scanner = new JsonFieldScanner();
        _smallPayloadUtf8 = Encoding.UTF8.GetBytes(BuildPayload(depth: 2, fieldCount: 8));
        _mediumPayloadUtf8 = Encoding.UTF8.GetBytes(BuildPayload(depth: 3, fieldCount: 25));
        _largePayloadUtf8 = Encoding.UTF8.GetBytes(BuildPayload(depth: 5, fieldCount: 120));

        _smallIndex = BuildMatcherIndex(matcherCount: 10, prefixLength: 10);
        _largeIndex = BuildMatcherIndex(matcherCount: 500, prefixLength: 20);

        _jexCompiler = new Jex();
        _jexProgram = _jexCompiler.Compile("{ \"result\": .amount * 2, \"currency\": .currency }");
        _jexInput = JObject.Parse(@"{""amount"": 150, ""currency"": ""ZAR""}");

        _smallRuleSet = new CompiledRuleSet(1,
            Enumerable.Range(0, 5).Select(i => new CompiledRule(i, 1, $"Rule_{i}")).ToList());
        _largeRuleSet = new CompiledRuleSet(1,
            Enumerable.Range(0, 50).Select(i => new CompiledRule(i, 1, $"Rule_{i}")).ToList());

        // Full pipeline common setup
        var matchers = new List<FunctionMatcherDefinition>
        {
            new(null, 1, 0, "Payment", new Regex("payment", RegexOptions.IgnoreCase)),
            new(null, 2, 0, "Transfer", new Regex("transfer", RegexOptions.IgnoreCase)),
        };

        _pipelineModel = new EventReaderRuntimeModel(
            version: 1,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new Dictionary<string, SourceSystemDefinition>
            {
                ["payments"] = new("payments", "payments-topic", "PaymentsProvider", true, new Dictionary<string, string>()),
            },
            functionDiscriminatorPaths: [JsonPathSelector.Create("$.Action")],
            clientIdentityPaths: [JsonPathSelector.Create("$.NedbankID")],
            functionMatchers: new FunctionMatcherIndex(matchers, 10),
            functions: new Dictionary<int, CompiledFunctionPlan>
            {
                [1] = new(1, 1, "TestFunction",
                    SourceSystemBehavior.Global,
                    new CompiledJexScript("", 1),
                    new CompiledRuleSet(1, Array.Empty<CompiledRule>()),
                    new OutputRoutePlan(1, "default", "output-topic"),
                    FunctionFailurePolicy.Default),
            });

        _pipelineScanner = new JsonFieldScanner();
        _pipelineIdentityResolver = new ClientIdentityResolver(new NoOpUsernameLookup());
        _pipelineShardAssigner = new ShardAssigner(16);
        _pipelineProcessor = new EventReaderWorkItemProcessor(_pipelineModel, _jexCompiler);

        _pipelineStoreTempPath = Path.Combine(Path.GetTempPath(), $"faster-pipeline-{Guid.NewGuid():N}");
        _pipelineStore = new FasterEventReaderWorkStore(new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_pipelineStoreTempPath, "log"),
            CheckpointPath = Path.Combine(_pipelineStoreTempPath, "chk"),
            CheckpointIntervalMs = 0,
        });
    }

    [GlobalCleanup]
    public async ValueTask GlobalCleanup()
    {
        if (_pipelineStore is IAsyncDisposable asyncDisp)
        {
            await asyncDisp.DisposeAsync();
        }
        else if (_pipelineStore is IDisposable disp)
        {
            disp.Dispose();
        }

        if (_pipelineStoreTempPath is not null && Directory.Exists(_pipelineStoreTempPath))
        {
            try { Directory.Delete(_pipelineStoreTempPath, recursive: true); } catch { }
        }
    }

    // ================================================================
    // ER-003.3: Scanner-only benchmarks
    // ================================================================

    [Benchmark(Description = "Scanner: Small payload, 1 selector")]
    public JsonFieldScanResult Scan_Small_1Selector()
    {
        return _scanner.Scan(_smallPayloadUtf8, OneSelector);
    }

    [Benchmark(Description = "Scanner: Small payload, 5 selectors")]
    public JsonFieldScanResult Scan_Small_5Selectors()
    {
        return _scanner.Scan(_smallPayloadUtf8, FiveSelectors);
    }

    [Benchmark(Description = "Scanner: Medium payload, 5 selectors")]
    public JsonFieldScanResult Scan_Medium_5Selectors()
    {
        return _scanner.Scan(_mediumPayloadUtf8, FiveSelectors);
    }

    [Benchmark(Description = "Scanner: Large payload, 5 selectors")]
    public JsonFieldScanResult Scan_Large_5Selectors()
    {
        return _scanner.Scan(_largePayloadUtf8, FiveSelectors);
    }

    // ================================================================
    // ER-003.3: FunctionID-only (classifier) benchmarks
    // ================================================================

    [Benchmark(Description = "Classifier: 10 matchers, prefix 10")]
    public FunctionMatchResult? Match_SmallIndex()
    {
        return _smallIndex.Match("payments", "Payment.Created");
    }

    [Benchmark(Description = "Classifier: 500 matchers, prefix 20")]
    public FunctionMatchResult? Match_LargeIndex()
    {
        return _largeIndex.Match("payments", "Payment.Created");
    }

    // ================================================================
    // ER-003.3: Combined scanner + classifier
    // ================================================================

    [Benchmark(Description = "Combined: Scan + Classify (medium payload)")]
    public (JsonFieldScanResult, FunctionMatchResult?) ScanAndClassify()
    {
        var scanResult = _scanner.Scan(_mediumPayloadUtf8, FiveSelectors);
        var discriminator = ResolveDiscriminatorValue(scanResult);
        var matchResult = _smallIndex.Match("payments", discriminator ?? "Payment.Created");
        return (scanResult, matchResult);
    }

    // ================================================================
    // ER-013.3: FASTER enqueue throughput benchmarks
    // ================================================================

    private IEventReaderWorkStore? _fasterEnqueueStore;
    private string? _fasterEnqueueTempPath;
    private ClassifiedWorkItem[]? _fasterEnqueueItems;

    [IterationSetup]
    public void FasterIterationSetup()
    {
        _fasterEnqueueTempPath = Path.Combine(Path.GetTempPath(), $"faster-bench-{Guid.NewGuid():N}");
        _fasterEnqueueStore = new FasterEventReaderWorkStore(new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_fasterEnqueueTempPath, "log"),
            CheckpointPath = Path.Combine(_fasterEnqueueTempPath, "chk"),
            CheckpointIntervalMs = 0,
        });
        _fasterEnqueueItems = Enumerable.Range(0, 10000)
            .Select(CreateClassifiedItem)
            .ToArray();
    }

    [IterationCleanup]
    public async ValueTask FasterIterationCleanup()
    {
        if (_fasterEnqueueStore is IAsyncDisposable asyncDisp)
        {
            await asyncDisp.DisposeAsync();
        }
        else if (_fasterEnqueueStore is IDisposable disp)
        {
            disp.Dispose();
        }

        _fasterEnqueueStore = null;

        if (_fasterEnqueueTempPath is not null && Directory.Exists(_fasterEnqueueTempPath))
        {
            try { Directory.Delete(_fasterEnqueueTempPath, recursive: true); } catch { }
        }

        _fasterEnqueueTempPath = null;
        _fasterEnqueueItems = null;
    }

    [Benchmark(Description = "FASTER: Enqueue 1000 records", OperationsPerInvoke = 1000)]
    public async Task Faster_Enqueue_1000()
    {
        var items = _fasterEnqueueItems!;
        var store = _fasterEnqueueStore!;
        for (var i = 0; i < 1000; i++)
        {
            await store.EnqueueClassifiedAsync(items[i], CancellationToken.None);
        }
    }

    [Benchmark(Description = "FASTER: Enqueue 10000 records", OperationsPerInvoke = 10000)]
    public async Task Faster_Enqueue_10000()
    {
        var items = _fasterEnqueueItems!;
        var store = _fasterEnqueueStore!;
        for (var i = 0; i < 10000; i++)
        {
            await store.EnqueueClassifiedAsync(items[i], CancellationToken.None);
        }
    }

    // ================================================================
    // ER-013.3: JEX extraction script benchmark
    // ================================================================

    [Benchmark(Description = "JEX: Simple extraction script")]
    public JToken Jex_Execute()
    {
        return _jexProgram.Execute(_jexInput.DeepClone());
    }

    // ================================================================
    // ER-013.3: Rules execution benchmarks
    // ================================================================

    [Benchmark(Description = "Rules: Execute 5 rules")]
    public List<RuleDummyOutput> Rules_5()
    {
        var outputs = new List<RuleDummyOutput>(_smallRuleSet.Rules.Count);
        foreach (var rule in _smallRuleSet.Rules)
        {
            outputs.Add(new RuleDummyOutput(rule.RuleId, rule.RuleVersion));
        }

        return outputs;
    }

    [Benchmark(Description = "Rules: Execute 50 rules")]
    public List<RuleDummyOutput> Rules_50()
    {
        var outputs = new List<RuleDummyOutput>(_largeRuleSet.Rules.Count);
        foreach (var rule in _largeRuleSet.Rules)
        {
            outputs.Add(new RuleDummyOutput(rule.RuleId, rule.RuleVersion));
        }

        return outputs;
    }

    // ================================================================
    // ER-013.3: Full pipeline benchmark (no Kafka)
    // ================================================================

    [Benchmark(Description = "FullPipeline: classify + enqueue + process + output", OperationsPerInvoke = 50)]
    public async Task FullPipeline_50()
    {
        for (var i = 0; i < 50; i++)
        {
            var payload = Encoding.UTF8.GetBytes(BuildActionPayload($"Payment.Created", 100000 + i));
            var scanResult = _pipelineScanner.Scan(payload,
                [.. _pipelineModel.FunctionDiscriminatorPaths, .. _pipelineModel.ClientIdentityPaths]);

            if (!scanResult.IsValidJson)
            {
                continue;
            }

            var discriminator = ResolveDiscriminatorValue(scanResult);
            if (discriminator is null)
            {
                continue;
            }

            var matchResult = _pipelineModel.FunctionMatchers.Match("payments", discriminator);
            if (matchResult is null)
            {
                continue;
            }

            var identityResult = await _pipelineIdentityResolver
                .ResolveAsync(scanResult, CancellationToken.None)
                .ConfigureAwait(false);

            var shardAssignment = _pipelineShardAssigner.Assign(identityResult.NedbankId);

            _pipelineModel.Functions.TryGetValue(matchResult.FunctionId, out var functionPlan);

            var classified = new ClassifiedWorkItem(
                workItemId: 0,
                source: new KafkaSourceIdentity("payments", "test-topic", 0, i, DateTimeOffset.UtcNow),
                functionId: matchResult.FunctionId,
                nedbankId: identityResult.NedbankId,
                shardId: shardAssignment.ShardId,
                runtimeModelVersion: _pipelineModel.Version,
                functionVersion: functionPlan?.FunctionVersion ?? 0,
                extractionScriptVersion: functionPlan?.ExtractionScript.Version ?? 0,
                ruleSetVersion: functionPlan?.RuleSet.Version ?? 0,
                outputRouteVersion: functionPlan?.OutputRoute.Version ?? 0,
                rawPayload: payload,
                createdUtc: DateTimeOffset.UtcNow);

            await _pipelineStore.EnqueueClassifiedAsync(classified, CancellationToken.None).ConfigureAwait(false);

            var leases = await _pipelineStore
                .LeaseShardBatchAsync(shardAssignment.ShardId, 1, TimeSpan.FromMinutes(1), CancellationToken.None)
                .ConfigureAwait(false);

            foreach (var lease in leases)
            {
                var result = await _pipelineProcessor
                    .ProcessAsync(lease, CancellationToken.None)
                    .ConfigureAwait(false);

                if (result.Success && result.OutputPayload is not null)
                {
                    await _pipelineStore
                        .MarkReadyToOutputAsync(lease.WorkItemId, result.OutputPayload, CancellationToken.None)
                        .ConfigureAwait(false);
                }
            }
        }
    }

    // ================================================================
    // Helpers
    // ================================================================

    private static string BuildPayload(int depth, int fieldCount)
    {
        var root = new Dictionary<string, object?>();
        PopulateObject(root, depth, fieldCount, "");
        return JsonSerializer.Serialize(root);
    }

    private static void PopulateObject(Dictionary<string, object?> dict, int remainingDepth, int remainingFields, string prefix)
    {
        if (remainingDepth <= 0 || remainingFields <= 0)
        {
            return;
        }

        var fieldsAtThisLevel = Math.Min(remainingFields, remainingDepth == 1 ? remainingFields : Math.Max(3, remainingFields / remainingDepth));
        var fieldsPlaced = 0;

        for (var i = 0; i < fieldsAtThisLevel && fieldsPlaced < remainingFields; i++)
        {
            var key = $"{prefix}field_{i}";

            if (remainingDepth > 1 && i == 0 && remainingFields > fieldsAtThisLevel)
            {
                var nested = new Dictionary<string, object?>();
                PopulateObject(nested, remainingDepth - 1, remainingFields - 1, $"{prefix}f{i}_");
                dict[key] = nested;
                fieldsPlaced++;
            }
            else
            {
                dict[key] = $"value_{prefix}{i}";
                fieldsPlaced++;
            }
        }

        if (prefix == "")
        {
            dict["Action"] = "Payment.Created";
            dict["NedbankID"] = "1000001";
            dict["correlationId"] = Guid.NewGuid().ToString();
            dict["source"] = "mobile";
            dict["version"] = "2.0";
            dict["amount"] = 150.75;
            dict["currency"] = "ZAR";
            dict["timestamp"] = DateTimeOffset.UtcNow.ToString("O");
        }
    }

    private static string BuildActionPayload(string action, long nedbankId)
    {
        return JsonSerializer.Serialize(new
        {
            Action = action,
            NedbankID = nedbankId.ToString(),
            correlationId = Guid.NewGuid().ToString(),
            source = "mobile",
            version = "2.0",
            amount = 150.75,
            currency = "ZAR",
            timestamp = DateTimeOffset.UtcNow.ToString("O"),
            metadata = new { origin = "app", region = "ZA" },
        });
    }

    private static FunctionMatcherIndex BuildMatcherIndex(int matcherCount, int prefixLength)
    {
        var matchers = new List<FunctionMatcherDefinition>(matcherCount);
        for (var i = 0; i < matcherCount; i++)
        {
            matchers.Add(new FunctionMatcherDefinition(
                null,
                i + 1,
                i,
                $"Matcher{i}",
                new Regex($"matcher{i}", RegexOptions.IgnoreCase)));
        }

        return new FunctionMatcherIndex(matchers, prefixLength);
    }

    private static string? ResolveDiscriminatorValue(JsonFieldScanResult scanResult)
    {
        foreach (var kvp in scanResult.Fields)
        {
            if (!string.IsNullOrEmpty(kvp.Value))
            {
                return kvp.Value;
            }
        }

        return null;
    }

    private static ClassifiedWorkItem CreateClassifiedItem(int i)
    {
        return new ClassifiedWorkItem(
            workItemId: i,
            source: new KafkaSourceIdentity("payments", "test-topic", 0, i, DateTimeOffset.UtcNow),
            functionId: (i % 10) + 1,
            nedbankId: 1000000 + i,
            shardId: i % 16,
            runtimeModelVersion: 1,
            functionVersion: 1,
            extractionScriptVersion: 1,
            ruleSetVersion: 1,
            outputRouteVersion: 1,
            rawPayload: Encoding.UTF8.GetBytes(BuildActionPayload("Payment.Created", 1000000 + i)),
            createdUtc: DateTimeOffset.UtcNow);
    }

    public sealed record RuleDummyOutput(int RuleId, long RuleVersion);

    private sealed class NoOpUsernameLookup : IUsernameIdentityLookup
    {
        public Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken)
        {
            return Task.FromResult<long?>(null);
        }
    }
}
