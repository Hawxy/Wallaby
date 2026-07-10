using System.Text;
using System.Text.Json;
using Confluent.Kafka;
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
        var names = WallabyNames.Unique();
        var topic = names.Named("products");

        await using var node = await WallabyTestNode.StartAsync(BuildServices(names, topic));
        // Deletes are only visible to the live stream (a backfill cannot see an already-deleted row),
        // so both changes must commit after the slot is streaming. Generous timeout: leader bootstrap
        // includes Kafka topic creation, which is slow on a cold broker under full-suite load.
        await WallabyReadiness.WaitForStreamingAsync(node.Services, TimeSpan.FromMinutes(2));

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"e2e_{names.Suffix}");
        await Db.DeleteProductAsync(id);

        var messages = Consume(topic, count: 2);
        messages.Count.ShouldBe(2);

        var upsert = messages[0];
        upsert.Message.Key.ShouldBe(id.ToString());
        Header(upsert, KafkaMessageWriter.OperationHeader).ShouldBe("upsert");
        using (var envelope = JsonDocument.Parse(upsert.Message.Value))
        {
            envelope.RootElement.GetProperty("id").GetString().ShouldBe(id.ToString());
            envelope.RootElement.GetProperty("document").GetProperty("name").GetString().ShouldBe($"e2e_{names.Suffix}");
            envelope.RootElement.GetProperty("metadata").GetProperty("table").GetString().ShouldBe("products");
        }

        var tombstone = messages[1];
        tombstone.Message.Key.ShouldBe(id.ToString());
        tombstone.Message.Value.ShouldBeNull();
        Header(tombstone, KafkaMessageWriter.OperationHeader).ShouldBe("delete");
        Header(tombstone, KafkaMessageWriter.TableHeader).ShouldBe("public.products");
    }

    [Test]
    public async Task Declared_topics_are_created_on_start_and_survive_a_leadership_takeover()
    {
        var names = WallabyNames.Unique();
        var topic = names.Named("products_cfg");

        // Two nodes initialize the same sink configuration; topic creation must be idempotent.
        await using var node1 = await WallabyTestNode.StartAsync(BuildServices(names, topic));
        await using var node2 = await WallabyTestNode.StartAsync(BuildServices(names, topic));
        // Either node may win leadership, so wait on the slot itself rather than one node's role.
        await WaitForSlotActiveAsync(names.Slot);

        var categoryId = await Db.AddCategoryAsync();
        var id = await Db.AddProductAsync(categoryId, $"cfg_{names.Suffix}");
        Consume(topic, count: 1).Count.ShouldBe(1);

        var admin = new AdminClientConfig { BootstrapServers = kafka.BootstrapServers };
        using var client = new AdminClientBuilder(admin).Build();
        var configs = await client.DescribeConfigsAsync(
            [new Confluent.Kafka.Admin.ConfigResource { Type = Confluent.Kafka.Admin.ResourceType.Topic, Name = topic }]);
        configs.Single().Entries["cleanup.policy"].Value.ShouldBe("compact");
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

    private List<ConsumeResult<string, byte[]>> Consume(string topic, int count)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = Guid.NewGuid().ToString("N"),
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(topic);

        var results = new List<ConsumeResult<string, byte[]>>();
        // Generous: a cold single-broker container can take a while to serve its first produce
        // (coordinator load, idempotence PID acquisition).
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(90);
        while (results.Count < count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result is not null)
            {
                results.Add(result);
            }
        }
        consumer.Close();
        return results;
    }

    private static string Header(ConsumeResult<string, byte[]> result, string key) =>
        Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes(key));
}
