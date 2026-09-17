using Wallaby.DependencyInjection;
using Wallaby.Sinks.Pgvector.Internal;

namespace Wallaby.Sinks.Pgvector;

/// <summary>Fluent helpers for registering a pgvector sink on a <see cref="WallabyBuilder"/>.</summary>
public static class PgvectorBuilderExtensions
{
    /// <summary>
    /// Register a pgvector sink under <paramref name="name"/>. Attach the entities it stores via
    /// <see cref="WallabySinkBuilder.WithMappings"/> on the returned builder; each mapping's
    /// destination is the table name.
    /// </summary>
    public static WallabySinkBuilder AddPgvectorSink(this WallabyBuilder builder, string name, Action<PgvectorSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new PgvectorSinkOptions { ConnectionString = "", Dimensions = 0 };
        configure(options);
        Validate(options);

        return builder.AddSink(name, _ => new PgvectorSink(name, options));
    }

    /// <summary>
    /// Provider-aware overload: <paramref name="configure"/> runs on first resolution, so option values
    /// can come from services (e.g. <c>IConfiguration</c>) while the registration itself stays eager.
    /// Validation failures surface at host start rather than at registration.
    /// </summary>
    public static WallabySinkBuilder AddPgvectorSink(this WallabyBuilder builder, string name, Action<IServiceProvider, PgvectorSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        return builder.AddSink(name, sp =>
        {
            var options = new PgvectorSinkOptions { ConnectionString = "", Dimensions = 0 };
            configure(sp, options);
            return new PgvectorSink(name, options);
        });
    }

    internal static void Validate(PgvectorSinkOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new WallabyConfigurationException("PgvectorSinkOptions.ConnectionString is required.");
        }
        if (options.Dimensions <= 0)
        {
            throw new WallabyConfigurationException("PgvectorSinkOptions.Dimensions must be positive.");
        }
        if (!PgvectorTables.IsValidIdentifier(options.Schema))
        {
            throw new WallabyConfigurationException(
                "PgvectorSinkOptions.Schema must be 1-63 characters of [a-zA-Z0-9_].");
        }
        if (options.DefaultTable is { } table && !PgvectorTables.IsValidIdentifier(table))
        {
            throw new WallabyConfigurationException(
                "PgvectorSinkOptions.DefaultTable must be 1-63 characters of [a-zA-Z0-9_].");
        }
        if (options.MaxRecordsPerRequest <= 0)
        {
            throw new WallabyConfigurationException("PgvectorSinkOptions.MaxRecordsPerRequest must be positive.");
        }
        if (options.MaxEmbeddingBatchSize <= 0)
        {
            throw new WallabyConfigurationException("PgvectorSinkOptions.MaxEmbeddingBatchSize must be positive.");
        }
        if (options.MaxEmbeddingConcurrency <= 0)
        {
            throw new WallabyConfigurationException("PgvectorSinkOptions.MaxEmbeddingConcurrency must be positive.");
        }

        var embedParts = (options.EmbeddingGenerator is not null, options.EmbedText is not null,
            !string.IsNullOrWhiteSpace(options.EmbeddingVersion));
        if (embedParts is not ((true, true, true) or (false, false, false)))
        {
            throw new WallabyConfigurationException(
                "PgvectorSinkOptions embedding requires EmbeddingGenerator, EmbedText, and EmbeddingVersion " +
                "together (or none of them, for transform-provided vectors via VectorField).");
        }
        if (options.EmbeddingGenerator is null && string.IsNullOrWhiteSpace(options.VectorField))
        {
            throw new WallabyConfigurationException(
                "PgvectorSinkOptions.VectorField must be a non-empty field name when no EmbeddingGenerator is set.");
        }
    }
}
