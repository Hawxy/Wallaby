using Wallaby.Abstractions;
using Wallaby.Model;

namespace Wallaby.Internal.Backfill;

/// <summary>
/// Describes a scoped (filtered) re-snapshot of a primary table: re-read the rows whose
/// <see cref="LookupColumns"/> match one of the distinct <see cref="LookupValues"/> tuples. Produced by
/// <c>DependentChangeResolver</c> when a dependent-table change fans out to more than one page of primary
/// rows; the tail is offloaded to a scoped backfill so the trigger transaction can be acknowledged
/// immediately. The scoped job re-snapshots the full filtered set (the inline first page is a fast,
/// idempotent head start).
/// </summary>
internal sealed record ScopedFanoutSpec(
    CapturedTable PrimaryTable,
    IReadOnlyList<string> LookupColumns,
    IReadOnlyList<object?[]> LookupValues);

/// <summary>
/// A persisted fan-out job as read from <c>wallaby.fanout_queue</c>. The lookup values and cursor are kept
/// as raw JSON; the worker coerces them to CLR types against the resolved primary table's columns.
/// <paramref name="Attempts"/> counts the failed runs so far and drives the job's retry backoff.
/// </summary>
internal sealed record FanoutJobRow(
    string TableQualified,
    string LookupHash,
    BackfillStatus Status,
    IReadOnlyList<string> LookupColumns,
    string LookupValuesJson,
    string? CursorJson,
    long RowsCopied,
    int Attempts = 0);
