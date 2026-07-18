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
    /// the moment it is persisted. The name is owned by the shared backfill contract.
    /// </summary>
    public const string BackfillNotifyChannel = Client.Internal.BackfillContract.NotifyChannel;

    /// <summary>
    /// LISTEN/NOTIFY channel signalled on suspend/resume transitions, so the runtime reacts the moment
    /// the control row changes. The name is owned by the shared control contract.
    /// </summary>
    public const string ControlNotifyChannel = Client.Internal.ControlContract.NotifyChannel;
}
