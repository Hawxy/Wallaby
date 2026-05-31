namespace Wallaby.Model;

/// <summary>
/// A single column value decoded out of a logical replication message. Values are fully read and
/// copied here inside the streaming loop, since Npgsql recycles the underlying message objects.
/// </summary>
public sealed class RawColumn
{
    /// <summary>The physical Postgres column name.</summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// The decoded value (string/byte[]/primitive as produced by the decoder), or <c>null</c> for
    /// SQL NULL or when <see cref="IsUnchangedToast"/> is true.
    /// </summary>
    public object? Value { get; init; }

    /// <summary>
    /// True when the value was an unchanged TOASTed value not carried in the WAL record (the column
    /// was not modified and <c>REPLICA IDENTITY</c> did not include it). Consumers must re-fetch it
    /// if needed rather than treat it as NULL.
    /// </summary>
    public bool IsUnchangedToast { get; init; }
}
