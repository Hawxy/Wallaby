namespace Wallaby.DependencyInjection;

/// <summary>
/// Declares an additional pgoutput publication + logical replication slot that Wallaby provisions and
/// keeps in sync for a third-party CDC consumer (e.g. an ELT tool) but never consumes itself. Configure
/// the tables the publication should contain (and, optionally, the publication name) via this builder.
/// </summary>
public sealed class ExternalSlotBuilder
{
    private readonly ExternalSlotRegistration _registration;

    internal ExternalSlotBuilder(ExternalSlotRegistration registration) => _registration = registration;

    /// <summary>
    /// Override the publication name. Defaults to <c>"{slotName}_pub"</c> when not set. Point your
    /// external tool at this publication (and the slot name) in pgoutput mode.
    /// </summary>
    public ExternalSlotBuilder WithPublication(string publicationName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(publicationName);
        _registration.PublicationName = publicationName;
        return this;
    }

    /// <summary>Include a table in the publication (schema defaults to <c>public</c>).</summary>
    public ExternalSlotBuilder ForTable(string table) => ForTable("public", table);

    /// <summary>Include a schema-qualified table in the publication.</summary>
    public ExternalSlotBuilder ForTable(string schema, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _registration.TableNames.Add((schema, table));
        return this;
    }

    /// <summary>
    /// Include the table mapped to <typeparamref name="TEntity"/>, resolved against the EF Core model
    /// at startup. Use <see cref="ForTable(string,string)"/> for tables that are not in the EF model.
    /// </summary>
    public ExternalSlotBuilder ForEntity<TEntity>() where TEntity : class
    {
        _registration.EntityTypes.Add(typeof(TEntity));
        return this;
    }
}
