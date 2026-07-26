using Microsoft.Extensions.Options;

namespace Wallaby.DependencyInjection;

/// <summary>
/// Validates the final <see cref="WallabyOptions"/> produced by the options pipeline (the builder's
/// <c>ConfigureOptions</c>/<c>UseConnectionString</c> actions composed with any
/// <c>Configure&lt;WallabyOptions&gt;</c>/<c>PostConfigure</c> registrations and configuration binding).
/// Runs on first resolution; failures surface as a <see cref="WallabyConfigurationException"/> from the
/// <see cref="WallabyOptions"/> singleton registration.
/// </summary>
internal sealed class WallabyOptionsValidator(WallabyConfiguration configuration) : IValidateOptions<WallabyOptions>
{
    public ValidateOptionsResult Validate(string? name, WallabyOptions options)
    {
        if (name is not null && name != Options.DefaultName)
        {
            return ValidateOptionsResult.Skip;
        }

        var failures = new List<string>();
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            failures.Add(
                "A connection string must be supplied: via UseConnectionString(...), " +
                "Configure<WallabyOptions>, or configuration binding.");
        }
        if (string.IsNullOrWhiteSpace(options.SlotName) || string.IsNullOrWhiteSpace(options.PublicationName))
        {
            failures.Add("SlotName and PublicationName must be non-empty.");
        }
        // Chunk rows and batches are fully materialized in memory, so both are capped.
        if (options.ChunkSize is <= 0 or > 100_000)
        {
            failures.Add("ChunkSize must be between 1 and 100000.");
        }
        if (options.MaxBatchSize is <= 0 or > 100_000)
        {
            failures.Add("MaxBatchSize must be between 1 and 100000.");
        }
        if (options.Advanced.MaxBufferedChangesPerTransaction <= 0)
        {
            failures.Add("Advanced.MaxBufferedChangesPerTransaction must be greater than zero.");
        }
        if (options.Advanced.MaxTransactionsPerBatch is <= 0 or > 10_000)
        {
            failures.Add("Advanced.MaxTransactionsPerBatch must be between 1 and 10000.");
        }
        if (options.Advanced.MaxFanoutKeysPerTransaction is <= 0 or > 1_000_000)
        {
            failures.Add("Advanced.MaxFanoutKeysPerTransaction must be between 1 and 1000000.");
        }
        if (options.Advanced.KeepaliveInterval <= TimeSpan.Zero)
        {
            failures.Add("Advanced.KeepaliveInterval must be greater than zero.");
        }
        if (options.Advanced.StandbyRetryInterval <= TimeSpan.Zero)
        {
            failures.Add("Advanced.StandbyRetryInterval must be greater than zero.");
        }
        if (options.Advanced.LeaderRetryInterval <= TimeSpan.Zero)
        {
            failures.Add("Advanced.LeaderRetryInterval must be greater than zero.");
        }
        if (options.Advanced.ControlPollInterval <= TimeSpan.Zero)
        {
            failures.Add("Advanced.ControlPollInterval must be greater than zero.");
        }
        if (options.Advanced.SuspensionAutoResumeGraceFloor < TimeSpan.Zero)
        {
            failures.Add("Advanced.SuspensionAutoResumeGraceFloor must be zero or greater.");
        }
        if (options.Advanced.FanoutPollInterval <= TimeSpan.Zero)
        {
            failures.Add("Advanced.FanoutPollInterval must be greater than zero.");
        }
        if (options.Advanced.BackfillPollInterval <= TimeSpan.Zero)
        {
            failures.Add("Advanced.BackfillPollInterval must be greater than zero.");
        }
        if (options.Advanced.CheckpointSaveInterval < TimeSpan.Zero)
        {
            failures.Add("Advanced.CheckpointSaveInterval must be zero or greater.");
        }
        if (options.Advanced.HeartbeatInterval < TimeSpan.Zero)
        {
            failures.Add("Advanced.HeartbeatInterval must be zero (disabled) or greater.");
        }
        if (options.Advanced.WatermarkVisibilityFenceTimeout < TimeSpan.Zero)
        {
            failures.Add("Advanced.WatermarkVisibilityFenceTimeout must be zero (disabled) or greater.");
        }
        if (options.SinkRetry is null)
        {
            failures.Add("SinkRetry must not be null.");
        }
        else
        {
            if (options.SinkRetry.MaxAttempts is < 0 or > 100)
            {
                failures.Add("SinkRetry.MaxAttempts must be between 0 and 100.");
            }
            if (options.SinkRetry.BaseDelay <= TimeSpan.Zero)
            {
                failures.Add("SinkRetry.BaseDelay must be greater than zero.");
            }
            if (options.SinkRetry.MaxDelay < options.SinkRetry.BaseDelay)
            {
                failures.Add("SinkRetry.MaxDelay must be at least SinkRetry.BaseDelay.");
            }
        }

        // External slots must not collide with the primary slot/publication (which only exists when
        // capturing). External-vs-external collisions are caught structurally in WallabyBuilder.Build().
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
