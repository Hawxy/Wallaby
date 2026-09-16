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

    /// <summary>
    /// Include every table the registered storage providers model (resolved at startup, so an entity added
    /// to the model joins the publication on the next start). Tables without a primary key are skipped.
    /// Narrow the set with <see cref="Except{TEntity}"/> / <see cref="Except(string,string)"/>; explicit
    /// <see cref="ForTable(string,string)"/> / <see cref="ForEntity{TEntity}"/> declarations are added on top.
    /// </summary>
    public ExternalSlotBuilder ForAllEntities()
    {
        _registration.AllEntities = true;
        return this;
    }

    /// <summary>
    /// Exclude the table mapped to <typeparamref name="TEntity"/> from a <see cref="ForAllEntities"/> set.
    /// The table must be one the model maps; excluding a table the slot also declares explicitly fails startup.
    /// </summary>
    public ExternalSlotBuilder Except<TEntity>() where TEntity : class
    {
        _registration.ExcludedEntityTypes.Add(typeof(TEntity));
        return this;
    }

    /// <summary>Exclude a table by name from a <see cref="ForAllEntities"/> set (schema defaults to <c>public</c>).</summary>
    public ExternalSlotBuilder Except(string table) => Except("public", table);

    /// <summary>
    /// Exclude a schema-qualified table from a <see cref="ForAllEntities"/> set. Names are case-sensitive and
    /// must match a table the model maps (a table with no entity type, such as a many-to-many join table, is
    /// excluded this way).
    /// </summary>
    public ExternalSlotBuilder Except(string schema, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _registration.ExcludedTableNames.Add((schema, table));
        return this;
    }
}
