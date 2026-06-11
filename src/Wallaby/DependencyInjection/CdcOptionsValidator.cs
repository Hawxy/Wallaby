using Microsoft.Extensions.Options;

namespace Wallaby.DependencyInjection;

/// <summary>
/// Validates the final <see cref="CdcOptions"/> produced by the options pipeline (the builder's
/// <c>ConfigureOptions</c>/<c>UseConnectionString</c> actions composed with any
/// <c>Configure&lt;CdcOptions&gt;</c>/<c>PostConfigure</c> registrations and configuration binding).
/// Runs on first resolution; failures surface as a <see cref="CdcConfigurationException"/> from the
/// <see cref="CdcOptions"/> singleton registration.
/// </summary>
internal sealed class CdcOptionsValidator(CdcConfiguration configuration) : IValidateOptions<CdcOptions>
{
    public ValidateOptionsResult Validate(string? name, CdcOptions options)
    {
        if (name is not null && name != Options.DefaultName)
        {
            return ValidateOptionsResult.Skip;
        }

        var failures = new List<string>();
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            failures.Add(
                "A connection string must be supplied — via UseConnectionString(...), " +
                "Configure<CdcOptions>, or configuration binding.");
        }
        if (string.IsNullOrWhiteSpace(options.SlotName) || string.IsNullOrWhiteSpace(options.PublicationName))
        {
            failures.Add("SlotName and PublicationName must be non-empty.");
        }
        if (options.ChunkSize <= 0)
        {
            failures.Add("ChunkSize must be greater than zero.");
        }
        if (options.MaxBatchSize <= 0)
        {
            failures.Add("MaxBatchSize must be greater than zero.");
        }
        if (options.MaxBufferedChangesPerTransaction <= 0)
        {
            failures.Add("MaxBufferedChangesPerTransaction must be greater than zero.");
        }
        if (options.KeepaliveInterval <= TimeSpan.Zero)
        {
            failures.Add("KeepaliveInterval must be greater than zero.");
        }
        if (options.LeaderHeartbeatInterval <= TimeSpan.Zero)
        {
            failures.Add("LeaderHeartbeatInterval must be greater than zero.");
        }

        // External slots must not collide with the primary slot/publication (which only exists when
        // capturing). External-vs-external collisions are caught structurally in CdcBuilder.Build().
        if (configuration.CaptureIntended)
        {
            foreach (var external in configuration.ExternalSlots)
            {
                if (string.Equals(external.SlotName, options.SlotName, StringComparison.Ordinal))
                {
                    failures.Add($"External slot name '{external.SlotName}' collides with the primary slot.");
                }
                if (string.Equals(external.ResolvedPublicationName, options.PublicationName, StringComparison.Ordinal))
                {
                    failures.Add($"External publication name '{external.ResolvedPublicationName}' collides with the primary publication.");
                }
            }
        }

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }
}
