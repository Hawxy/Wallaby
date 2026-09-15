using Wallaby.Abstractions;

namespace Wallaby.Sinks;

/// <summary>
/// The destination fallback shared by the built-in sinks (and available to custom sinks): the record's
/// own destination, else the sink's configured default, else a <see cref="WallabyConfigurationException"/>
/// naming the mapping and the sink option that would resolve it.
/// </summary>
public static class SinkDestination
{
    /// <summary>Resolve the destination for a routed record.</summary>
    /// <param name="record">The record being delivered.</param>
    /// <param name="sinkDefault">The sink's default destination option value.</param>
    /// <param name="sinkName">The sink's registration name, for the error message.</param>
    /// <param name="defaultOptionName">The sink option holding <paramref name="sinkDefault"/>, e.g. <c>DefaultIndex</c>.</param>
    public static string Resolve(SinkRecord record, string? sinkDefault, string sinkName, string defaultOptionName)
        => record.Destination ?? sinkDefault
            ?? throw new WallabyConfigurationException(
                $"Record {record.DocumentId} from {record.Metadata.QualifiedTableName} has no destination and " +
                $"sink '{sinkName}' has no {defaultOptionName}. Set ToDestination(...) on the mapping or " +
                $"{defaultOptionName} on the sink.");

    /// <summary>Resolve the destination for a purge request.</summary>
    /// <param name="request">The purge being served.</param>
    /// <param name="sinkDefault">The sink's default destination option value.</param>
    /// <param name="sinkName">The sink's registration name, for the error message.</param>
    /// <param name="defaultOptionName">The sink option holding <paramref name="sinkDefault"/>, e.g. <c>DefaultIndex</c>.</param>
    public static string Resolve(SinkPurgeRequest request, string? sinkDefault, string sinkName, string defaultOptionName)
        => request.Destination ?? sinkDefault
            ?? throw new WallabyConfigurationException(
                $"A purge for {request.QualifiedTableName} has no destination and sink '{sinkName}' has no " +
                $"{defaultOptionName}. Set ToDestination(...) on the mapping or {defaultOptionName} on the sink.");
}
