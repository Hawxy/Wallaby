namespace Wallaby.Abstractions;

/// <summary>
/// The kind of change represented by a <see cref="ChangeEvent"/>.
/// </summary>
/// <remarks>Member names map to the sink-envelope <c>action</c> strings, a wire contract; never rename.</remarks>
public enum ChangeAction
{
    /// <summary>A row was inserted.</summary>
    Insert,

    /// <summary>A row was updated.</summary>
    Update,

    /// <summary>A row was deleted.</summary>
    Delete,

    /// <summary>
    /// A row read during an initial/backfill snapshot (not a live WAL change).
    /// Mirrors Sequin's "read" action.
    /// </summary>
    Read,
}
