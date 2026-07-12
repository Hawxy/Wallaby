using System.Data.Common;
using Npgsql.Replication.PgOutput;
using Wallaby.Model;

namespace Wallaby.Internal;

/// <summary>
/// The single implementation of <see cref="ColumnReadMode"/> for both read paths: backfill
/// (<see cref="DbDataReader"/>) and replication (<see cref="ReplicationValue"/>). NULL handling
/// lives here too, since it differs per path and per mode.
/// </summary>
internal static class ColumnValueReader
{
    public static object? Read(DbDataReader reader, int ordinal, ColumnReadMode mode)
    {
        if (mode == ColumnReadMode.Utf8JsonBytes)
        {
            // GetFieldValue<byte[]> throws on NULL (unlike GetValue), so guard explicitly.
            return reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<byte[]>(ordinal);
        }

        var value = reader.GetValue(ordinal);
        return value is DBNull ? null : value;
    }

    public static async ValueTask<object?> ReadAsync(ReplicationValue value, ColumnReadMode mode, CancellationToken ct)
    {
        if (value.IsDBNull)
        {
            // Consume the (empty) value to keep the tuple stream positioned correctly.
            _ = await value.Get(ct);
            return null;
        }

        return mode == ColumnReadMode.Utf8JsonBytes ? await value.Get<byte[]>(ct) : await value.Get(ct);
    }
}
