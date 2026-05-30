namespace EFCore.CDC.Internal;

/// <summary>Names of the library's internal schema and the watermark message prefixes.</summary>
internal static class CdcSchema
{
    public const string Schema = "cdc";

    /// <summary>Root prefix for generic WAL messages used as backfill watermarks.</summary>
    public const string WatermarkPrefix = "cdc.watermark";
    public const string WatermarkLowPrefix = WatermarkPrefix + ".low";
    public const string WatermarkHighPrefix = WatermarkPrefix + ".high";
}
