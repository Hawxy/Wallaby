using Microsoft.Extensions.DependencyInjection;
using Wallaby.Abstractions;
using Wallaby.DependencyInjection;

namespace Wallaby.Tests.Unit;

/// <summary>Builder arguments fail fast at registration instead of blowing up mid-stream.</summary>
public class BuilderArgumentTests
{
    private sealed class Doc { public int Id { get; set; } }

    private static EntityMapBuilder<Doc> Map() => new(new MappingRegistration { EntityClrType = typeof(Doc) });

    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments(" ")]
    public void Backfill_version_must_have_content(string? version)
    {
        Should.Throw<ArgumentException>(() => Map().WithBackfillVersion(version!));
    }

    [Test]
    public void Scoped_by_entity_selector_must_not_be_null()
    {
        Should.Throw<ArgumentNullException>(() => Map().ScopedBy((Func<Doc, object?>)null!));
    }

    [Test]
    public void Scoped_destination_selector_must_not_be_null()
    {
        Should.Throw<ArgumentNullException>(() => Map().ScopedDestination(null!));
    }

    [Test]
    public void A_null_sink_instance_is_rejected()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentNullException>(() => builder.AddSink(null!));
    }

    [Test]
    [Arguments(null)]
    [Arguments(" ")]
    public void A_factory_sink_requires_a_name(string? name)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.AddSink(name!, _ => null!));
    }

    [Test]
    public void A_factory_sink_requires_a_factory()
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentNullException>(() => builder.AddSink("sink", null!));
    }

    [Test]
    [Arguments(null)]
    [Arguments("")]
    public void A_literal_connection_string_must_have_content(string? connectionString)
    {
        var builder = new WallabyBuilder(new ServiceCollection());

        Should.Throw<ArgumentException>(() => builder.UseConnectionString(connectionString!));
    }
}
