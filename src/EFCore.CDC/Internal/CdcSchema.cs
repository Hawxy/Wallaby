namespace EFCore.CDC.Internal;

/// <summary>Names of the library's internal schema and tables in the source database.</summary>
internal static class CdcSchema
{
    public const string Schema = "cdc";
    public const string WatermarkTable = "watermark";

    /// <summary>Schema-qualified watermark table, e.g. for matching decoded changes.</summary>
    public const string WatermarkSchema = Schema;
}
