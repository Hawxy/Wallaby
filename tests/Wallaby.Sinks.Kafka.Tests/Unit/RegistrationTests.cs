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

        Should.Throw<ArgumentException>(() => builder.AddKafkaSink("kafka", _ => { }))
            .Message.ShouldContain("BootstrapServers");
    }

    [Test]
    public void Message_timeout_must_be_positive()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.MessageTimeoutMs = 0;
        }));
    }

    [Test]
    public void Linger_cannot_be_negative()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddKafkaSink("kafka", o =>
        {
            o.BootstrapServers = "broker:9092";
            o.LingerMs = -1;
        }));
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
