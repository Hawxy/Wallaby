namespace Wallaby.Model;

/// <summary>
/// How core reads a column's wire value into <see cref="RawColumn.Value"/>, on both the
/// replication and backfill paths. Declared by the storage provider on the capture model;
/// the read itself is implemented once in core.
/// </summary>
public enum ColumnReadMode
{
    /// <summary>Npgsql's default CLR mapping (jsonb → string, etc.). NULL becomes null.</summary>
    Default = 0,

    /// <summary>
    /// Raw UTF-8 JSON bytes (<c>byte[]</c>) instead of a decoded string. Only valid on json/jsonb
    /// columns; lets a consumer that feeds a JSON deserializer skip the UTF-16 round trip.
    /// </summary>
    Utf8JsonBytes,
}
