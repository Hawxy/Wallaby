using Wallaby.Client;

namespace Wallaby.Aspire.Hosting.Internal;

/// <summary>One poll of a Wallaby installation's control plane.</summary>
/// <param name="Control">The suspension state and managed slots.</param>
/// <param name="Backfills">Every tracked table's backfill state.</param>
/// <param name="Error">Why the most recent poll failed; <paramref name="Control"/> and <paramref name="Backfills"/> then hold the last good read.</param>
/// <param name="PreviouslyCaughtUp">
/// Whether the previous poll was already caught up. A host's first backfill rows appear only after its
/// slot starts streaming, so a single caught-up poll can precede them; health waits for a second one.
/// </param>
internal sealed record WallabyObservation(
    WallabyControlState Control,
    IReadOnlyList<WallabyBackfillState> Backfills,
    string? Error = null,
    bool PreviouslyCaughtUp = false);
