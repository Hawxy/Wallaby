namespace Wallaby.Client;

/// <summary>
/// The database's <c>wallaby</c> schema is older than this client requires for the requested operation
/// (or no Wallaby host has ever run against it). Deploy a newer host first; it migrates the schema at
/// startup.
/// </summary>
public sealed class WallabySchemaVersionException : InvalidOperationException
{
    internal WallabySchemaVersionException(int foundVersion, int requiredVersion, string message)
        : base(message)
    {
        FoundVersion = foundVersion;
        RequiredVersion = requiredVersion;
    }

    /// <summary>The schema version found in the database; zero when the version ledger is absent.</summary>
    public int FoundVersion { get; }

    /// <summary>The schema version the operation requires.</summary>
    public int RequiredVersion { get; }
}
