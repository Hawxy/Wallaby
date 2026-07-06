using System.Diagnostics.CodeAnalysis;
using Wallaby.Model;

namespace Wallaby.Providers.Marten.Internal;

/// <summary>
/// Per-document materialization plan shared by <see cref="MartenModelProvider"/> (which derives it from
/// the store's document mappings) and <see cref="MartenRowMaterializer"/> (which executes it per change).
/// </summary>
internal sealed class MartenTablePlan
{
    public required CapturedTable Table { get; init; }

    /// <summary>The document CLR type <c>data</c> deserializes into.</summary>
    public required Type DocumentType { get; init; }

    /// <summary>The document's Id member name (the record key for the id value).</summary>
    public required string IdPropertyName { get; init; }

    public required Type IdType { get; init; }

    /// <summary>True for conjoined tenancy: <c>tenant_id</c> participates in the key and the record.</summary>
    [MemberNotNullWhen(true, nameof(TenantColumnName))]
    public bool Conjoined => TenantColumnName is not null;

    /// <summary>True when the document uses soft deletes (<c>mt_deleted</c> flips instead of a DELETE).</summary>
    [MemberNotNullWhen(true, nameof(DeletedColumnName))]
    public bool SoftDeleted => DeletedColumnName is not null;

    /// <summary>The tenant-id column name for conjoined tenancy, else null.</summary>
    public required string? TenantColumnName { get; init; }

    /// <summary>The soft-delete flag column name, else null.</summary>
    public required string? DeletedColumnName { get; init; }
}
