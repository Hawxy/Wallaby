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
            Validate(options);
            return new PgvectorSink(name, options);
        });
    }

    internal static void Validate(PgvectorSinkOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("PgvectorSinkOptions.ConnectionString is required.", nameof(options));
        }
        if (options.Dimensions <= 0)
        {
            throw new ArgumentException("PgvectorSinkOptions.Dimensions must be positive.", nameof(options));
        }
        if (!PgvectorTables.IsValidIdentifier(options.Schema))
        {
            throw new ArgumentException(
                "PgvectorSinkOptions.Schema must be 1-63 characters of [a-zA-Z0-9_].", nameof(options));
        }
        if (options.DefaultTable is { } table && !PgvectorTables.IsValidIdentifier(table))
        {
            throw new ArgumentException(
                "PgvectorSinkOptions.DefaultTable must be 1-63 characters of [a-zA-Z0-9_].", nameof(options));
        }
        if (options.MaxRowsPerBatch <= 0)
        {
            throw new ArgumentException("PgvectorSinkOptions.MaxRowsPerBatch must be positive.", nameof(options));
        }
        if (options.MaxEmbeddingBatchSize <= 0)
        {
            throw new ArgumentException("PgvectorSinkOptions.MaxEmbeddingBatchSize must be positive.", nameof(options));
        }
        if (options.MaxEmbeddingConcurrency <= 0)
        {
            throw new ArgumentException("PgvectorSinkOptions.MaxEmbeddingConcurrency must be positive.", nameof(options));
        }

        var embedParts = (options.EmbeddingGenerator is not null, options.EmbedText is not null,
            !string.IsNullOrWhiteSpace(options.EmbeddingVersion));
        if (embedParts is not ((true, true, true) or (false, false, false)))
        {
            throw new ArgumentException(
                "PgvectorSinkOptions embedding requires EmbeddingGenerator, EmbedText, and EmbeddingVersion " +
                "together (or none of them, for transform-provided vectors via VectorField).", nameof(options));
        }
        if (options.EmbeddingGenerator is null && string.IsNullOrWhiteSpace(options.VectorField))
        {
            throw new ArgumentException(
                "PgvectorSinkOptions.VectorField must be a non-empty field name when no EmbeddingGenerator is set.",
                nameof(options));
        }
    }
}
