namespace EFCore.CDC.Testing;

/// <summary>Unique slot/publication (and a reusable suffix for index/table names) for an isolated test.</summary>
public sealed record CdcNames(string Suffix, string Slot, string Publication)
{
    public static CdcNames Unique()
    {
        var suffix = Guid.NewGuid().ToString("N");
        return new CdcNames(suffix, $"cdc_slot_{suffix}", $"cdc_pub_{suffix}");
    }

    /// <summary>A unique, suffixed name (e.g. for a sink destination/index): <c>{prefix}_{suffix}</c>.</summary>
    public string Named(string prefix) => $"{prefix}_{Suffix}";
}
