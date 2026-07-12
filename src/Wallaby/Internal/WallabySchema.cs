namespace Wallaby.Internal;

/// <summary>Names of the library's internal schema and the watermark message prefixes.</summary>
internal static class WallabySchema
{
    public const string Schema = "wallaby";

    /// <summary>Root prefix for generic WAL messages used as backfill watermarks.</summary>
    public const string WatermarkPrefix = Schema + ".watermark";
    public const string WatermarkLowPrefix = WatermarkPrefix + ".low";
    public const string WatermarkHighPrefix = WatermarkPrefix + ".high";

    /// <summary>
    /// LISTEN/NOTIFY channel the fan-out queue uses to wake its worker the moment a job is enqueued.
    /// </summary>
    public const string FanoutNotifyChannel = "wallaby_fanout";

    /// <summary>
    /// LISTEN/NOTIFY channel a manual backfill request signals, so the leader's scheduler serves it
    /// the moment it is persisted.
    /// </summary>
    public const string BackfillNotifyChannel = "wallaby_backfill";
}
