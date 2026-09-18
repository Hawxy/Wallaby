using Microsoft.Extensions.DependencyInjection;
using Wallaby.DependencyInjection;

namespace Wallaby.Sinks.Kafka.Tests.Unit;

/// <summary>Validation requirements of <c>AddKafkaSink</c>.</summary>
public class RegistrationTests
{
    [Test]
    public void Bootstrap_servers_are_required()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", _ => { }))
            .Message.ShouldContain("BootstrapServers");
    }

    [Test]
    public void Message_timeout_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.MessageTimeout = TimeSpan.Zero;
        }));
    }

    [Test]
    public void Linger_cannot_be_negative()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.Linger = TimeSpan.FromMilliseconds(-1);
        }));
    }

    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public void Topic_partitions_must_be_positive(int partitions)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.Topics.Add(new KafkaTopicConfig { Name = "orders", Partitions = partitions });
        })).Message.ShouldContain("Partitions");
    }

    [Test]
    [Arguments((short)0)]
    [Arguments((short)-2)]
    public void Topic_replication_factor_must_be_positive_or_broker_default(short replicationFactor)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.Topics.Add(new KafkaTopicConfig { Name = "orders", ReplicationFactor = replicationFactor });
        })).Message.ShouldContain("ReplicationFactor");
    }

    [Test]
    public void Topic_name_is_required()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.Topics.Add(new KafkaTopicConfig { Name = " " });
        })).Message.ShouldContain("Name");
    }

    [Test]
    public void An_undefined_compression_value_is_rejected()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<WallabyConfigurationException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.Compression = (KafkaSinkCompression)42;
        })).Message.ShouldContain("Compression");
    }

    [Test]
    public void The_constructor_validates_options()
    {
        Should.Throw<WallabyConfigurationException>(
            () => new KafkaSink("kafka", new KafkaSinkOptions { BootstrapServers = " " }));
    }

    [Test]
    public void A_topic_with_defaults_is_valid()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.Topics.Add(new KafkaTopicConfig { Name = "orders" }); // Partitions 1, ReplicationFactor -1
        }).ShouldNotBeNull();
    }

    [Test]
    public void A_valid_configuration_registers_the_sink()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.DefaultTopic = "wallaby";
        }).ShouldNotBeNull();
    }
}
