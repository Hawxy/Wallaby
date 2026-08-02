namespace Wallaby.Providers;

/// <summary>
/// Thrown by a provider materializer when a change omits a value that was never on the wire: an
/// unchanged TOASTed column under <c>REPLICA IDENTITY DEFAULT</c> with no old tuple to fall back to.
/// </summary>
/// <remarks>
/// When <c>WallabyOptions.ReselectUnavailableValues</c> is enabled (the default) the pipeline heals
/// the change by re-reading the row by primary key. The re-read returns current row state, not
/// commit-time state; later updates to the row are themselves in the stream, so sinks converge
/// forward. When disabled, the change is a poison change that halts the pipeline.
/// </remarks>
public sealed class UnavailableValueException : InvalidOperationException
{
    /// <summary>Creates the exception for one unavailable column of one change.</summary>
    public UnavailableValueException(string schema, string tableName, string columnName, string message)
        : base(message)
    {
        Schema = schema;
        TableName = tableName;
        ColumnName = columnName;
    }

    /// <summary>Schema of the table whose change omitted the value.</summary>
    public string Schema { get; }

    /// <summary>Name of the table whose change omitted the value.</summary>
    public string TableName { get; }

    /// <summary>The column whose value was not carried in the change.</summary>
    public string ColumnName { get; }
}
