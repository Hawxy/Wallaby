using System.Text;

namespace Wallaby.Internal;

/// <summary>Startup validation of the replication slot and publication names Wallaby creates.</summary>
internal static class PgNames
{
    /// <summary>Postgres identifiers are truncated past this many bytes, so a longer name never matches on reconcile.</summary>
    public const int MaxIdentifierBytes = 63;

    /// <summary>The server accepts only lower-case letters, digits and underscores in a slot name.</summary>
    public static bool IsValidSlotName(string name)
    {
        if (name.Length is 0 or > MaxIdentifierBytes)
        {
            return false;
        }
        foreach (var c in name)
        {
            if (c is not ((>= 'a' and <= 'z') or (>= '0' and <= '9') or '_'))
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>Publication names are quoted in DDL, so any non-empty name within the byte limit works.</summary>
    public static bool IsValidPublicationName(string name)
        => !string.IsNullOrWhiteSpace(name) && Encoding.UTF8.GetByteCount(name) <= MaxIdentifierBytes;

    public static string SlotNameRule => "1-63 lower-case letters, digits and underscores";

    public static string PublicationNameRule => "non-empty and at most 63 bytes";
}
