namespace Wallaby;

/// <summary>
/// Thrown when CDC cannot be configured against the EF Core model or the Postgres server — e.g. a
/// declared entity has no primary key, no tables were declared, or a required server setting is wrong.
/// Messages are intended to be actionable.
/// </summary>
public sealed class CdcConfigurationException : Exception
{
    /// <summary>Creates the exception with a message.</summary>
    public CdcConfigurationException(string message) : base(message) { }

    /// <summary>Creates the exception with a message and inner exception.</summary>
    public CdcConfigurationException(string message, Exception innerException) : base(message, innerException) { }
}
