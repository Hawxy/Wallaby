using Wallaby.Model;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>
/// Per-table materialization plan: the captured members (after column selection) with a column-name
/// index, the key members, and the constructor plan over the registration's full member list.
/// </summary>
internal sealed class TablePlan
{
    public required CapturedTable Table { get; init; }

    public required TableRegistration Registration { get; init; }

    /// <summary>Captured members in declaration order; a narrowed selection omits the rest.</summary>
    public required IReadOnlyList<MemberPlan> Captured { get; init; }

    /// <summary>Column name to index into <see cref="Captured"/>.</summary>
    public required IReadOnlyDictionary<string, int> ColumnsByName { get; init; }

    /// <summary>Per constructor parameter, the index into <see cref="Captured"/>, or -1 when not captured.</summary>
    public required int[] ConstructorArguments { get; init; }

    /// <summary>Indices into <see cref="Captured"/> assigned through their setter after construction.</summary>
    public required int[] Assignments { get; init; }
}
