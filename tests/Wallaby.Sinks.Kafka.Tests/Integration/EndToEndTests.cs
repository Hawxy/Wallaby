using System.Text.Json;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Wallaby.DependencyInjection;
using Wallaby.Providers.EntityFrameworkCore;
using Wallaby.Sinks.Kafka.Internal;
using Wallaby.Sinks.Kafka.Tests.Integration.Infrastructure;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestInfrastructure;
using Wallaby.Testing;
using Wallaby.TestModel;
using DekafKafka = Dekaf.Kafka;

namespace Wallaby.Sinks.Kafka.Tests.Integration;

[NotInParallel]
[ClassDataSource<TestModelPostgresFixture, KafkaFixture>(Shared = new[] { SharedType.PerTestSession, SharedType.PerTestSession })]
public class EndToEndTests(TestModelPostgresFixture pg, KafkaFixture kafka)
{
    private TestDatabase Db => new(pg.ConnectionString);

    private ServiceCollection BuildServices(WallabyNames names, string topic)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pg.ConnectionString));
        services.AddWallaby(cdc =>
        {
            cdc.UseEntityFrameworkCore<AppDbContext>()
               .UseConnectionString(pg.ConnectionString)
               .ConfigureOptions(o =>
               {
                   o.SlotName = names.Slot;
                   o.PublicationName = names.Publication;
                   o.Advanced.StandbyRetryInterval = TimeSpan.FromSeconds(1);
               })
               .AddKafkaSink("kafka", k =>
               {
                   k.BootstrapServers = kafka.BootstrapServers;
                   k.Topics.Add(new KafkaTopicConfig
                   {
                       Name = topic,
                       Config = { ["cleanup.policy"] = "compact" },
                   });
               })
               .WithMappings(sink => sink
                   .Map<Product>()
                   .ToDestination(topic)
                   .UsingTransform(TestTransforms.ProductNames));
        });
        return services;
    }

    [Test]
    public async Task Changes_are_produced_end_to_end_and_deletes_are_tombstones()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var topic = names.Named("products");

        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, topic));
        // Deletes are only visible to the live stream (a backfill cannot see an already-deleted row),
        // so both changes must commit after the slot is streaming. Generous timeout: leader bootstrap
        // includes Kafka topic creation, which is slow on a cold broker under full-suite load.
        await WallabyReadiness.WaitForStreamingAsync(node.Services, TimeSpan.FromMinutes(2));

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");

        // Consume the upsert before deleting: the router collapses same-key changes within one
        // coalesced batch to their final action, so an insert+delete landing in a single batch
        // delivers only the tombstone.
        var upserts = await ConsumeAsync(topic, count: 1);
        upserts.Count.ShouldBe(1);

        var upsert = upserts[0];
        upsert.Key.ShouldBe(id.ToString());
        Header(upsert, KafkaMessageWriter.OperationHeader).ShouldBe("upsert");
        using (var envelope = JsonDocument.Parse(upsert.Value))
        {
            envelope.RootElement.GetProperty("id").GetString().ShouldBe(id.ToString());
            envelope.RootElement.GetProperty("document").GetProperty("name").GetString().ShouldBe($"e2e_{names.Suffix}");
            envelope.RootElement.GetProperty("metadata").GetProperty("table").GetString().ShouldBe("products");
        }

        await Db.DeleteProductAsync(id);

        var messages = await ConsumeAsync(topic, count: 2);
        messages.Count.ShouldBe(2);

        var tombstone = messages[1];
        tombstone.Key.ShouldBe(id.ToString());
        tombstone.Value.ShouldBeNull();
        Header(tombstone, KafkaMessageWriter.OperationHeader).ShouldBe("delete");
        Header(tombstone, KafkaMessageWriter.TableHeader).ShouldBe("public.products");
    }

    [Test]
    public async Task Declared_topics_are_created_on_start_and_survive_a_leadership_takeover()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var topic = names.Named("products_cfg");

        // Two nodes initialize the same sink configuration; topic creation must be idempotent.
        await using var node1 = await WallabyTestNode.StartAsync(BuildServices(names, topic));
        await using var node2 = await WallabyTestNode.StartAsync(BuildServices(names, topic));
        // Either node may win leadership, so wait on the slot itself rather than one node's role.
        await WaitForSlotActiveAsync(names.Slot);

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"cfg_{names.Suffix}");
        (await ConsumeAsync(topic, count: 1)).Count.ShouldBe(1);

        await using var admin = DekafKafka.CreateAdminClient()
            .WithBootstrapServers(kafka.BootstrapServers)
            .Build();
        var configs = await admin.DescribeConfigsAsync([ConfigResource.Topic(topic)]);
        configs.Single().Value.Single(e => e.Name == "cleanup.policy").Value.ShouldBe("compact");
    }

    private async Task WaitForSlotActiveAsync(string slot)
    {
        await using var dataSource = NpgsqlDataSource.Create(pg.ConnectionString);
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
        while (true)
        {
            await using (var command = dataSource.CreateCommand(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1"))
            {
                command.Parameters.AddWithValue(slot);
                if (await command.ExecuteScalarAsync() is true)
                {
                    return;
                }
            }

            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException($"Replication slot '{slot}' never became active.");
            }
            await Task.Delay(100);
        }
    }

    private sealed record ConsumedMessage(string? Key, byte[]? Value, IReadOnlyDictionary<string, string?> Headers);

    private async Task<List<ConsumedMessage>> ConsumeAsync(string topic, int count)
    {
        await using var consumer = await DekafKafka.CreateConsumer<string, byte[]>()
            .WithBootstrapServers(kafka.BootstrapServers)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .BuildAsync();
        // Manual assignment instead of a subscription: sink topics have one partition, and skipping the
        // consumer group avoids coordinator load and __consumer_offsets creation on the cold broker
        // (the dominant source of consume timeouts under full-suite load).
        consumer.Partitions.Assign([new TopicPartition(topic, 0)]);

        var results = new List<ConsumedMessage>();
        // Generous: a cold single-broker container can take a while to serve its first produce
        // (coordinator load, idempotence PID acquisition).
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
        while (results.Count < count && DateTime.UtcNow < deadline)
        {
            var result = await consumer.ConsumeOneAsync(TimeSpan.FromMilliseconds(500));
            if (result is { } message)
            {
                // Headers are a lazy view over the pooled fetch batch; snapshot everything before the
                // next poll invalidates them.
                var headers = new Dictionary<string, string?>();
                foreach (var header in message.Headers)
                {
                    headers[header.Key] = header.GetValueAsString();
                }
                results.Add(new ConsumedMessage(message.Key, message.Value, headers));
            }
        }
        return results;
    }

    private static string? Header(ConsumedMessage message, string key)
        => message.Headers.GetValueOrDefault(key);
}
