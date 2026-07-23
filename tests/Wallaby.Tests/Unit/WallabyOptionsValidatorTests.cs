using Wallaby.DependencyInjection;

namespace Wallaby.Tests.Unit;

public class WallabyOptionsValidatorTests
{
    private static WallabyOptions ValidOptions() => new() { ConnectionString = "Host=localhost" };

    private static Microsoft.Extensions.Options.ValidateOptionsResult Validate(WallabyOptions options)
        => new WallabyOptionsValidator(new WallabyConfiguration()).Validate(null, options);

    [Test]
    public void Valid_options_pass()
    {
        Validate(ValidOptions()).Succeeded.ShouldBeTrue();
    }

    [Test]
    public void Sink_retry_attempts_out_of_range_fail()
    {
        var options = ValidOptions();
        options.SinkRetry.MaxAttempts = -1;
        Validate(options).Failed.ShouldBeTrue();

        options.SinkRetry.MaxAttempts = 101;
        Validate(options).Failed.ShouldBeTrue();

        options.SinkRetry.MaxAttempts = 0;
        Validate(options).Succeeded.ShouldBeTrue();

        options.SinkRetry.MaxAttempts = 100;
        Validate(options).Succeeded.ShouldBeTrue();
    }

    [Test]
    public void Chunk_size_is_capped()
    {
        var options = ValidOptions();
        options.ChunkSize = 100_000;
        Validate(options).Succeeded.ShouldBeTrue();

        options.ChunkSize = 100_001;
        Validate(options).Failed.ShouldBeTrue();
    }

    [Test]
    public void Max_batch_size_is_capped()
    {
        var options = ValidOptions();
        options.MaxBatchSize = 100_000;
        Validate(options).Succeeded.ShouldBeTrue();

        options.MaxBatchSize = 100_001;
        Validate(options).Failed.ShouldBeTrue();
    }

    [Test]
    public void Max_transactions_per_batch_is_capped()
    {
        var options = ValidOptions();
        options.Advanced.MaxTransactionsPerBatch = 1;
        Validate(options).Succeeded.ShouldBeTrue();

        options.Advanced.MaxTransactionsPerBatch = 10_001;
        Validate(options).Failed.ShouldBeTrue();

        options.Advanced.MaxTransactionsPerBatch = 0;
        Validate(options).Failed.ShouldBeTrue();
    }

    [Test]
    public void Negative_checkpoint_save_interval_fails()
    {
        var options = ValidOptions();
        options.Advanced.CheckpointSaveInterval = TimeSpan.FromSeconds(-1);
        Validate(options).Failed.ShouldBeTrue();

        options.Advanced.CheckpointSaveInterval = TimeSpan.Zero;
        Validate(options).Succeeded.ShouldBeTrue();
    }

    [Test]
    public void Negative_heartbeat_interval_fails()
    {
        var options = ValidOptions();
        options.Advanced.HeartbeatInterval = TimeSpan.FromSeconds(-1);
        Validate(options).Failed.ShouldBeTrue();

        options.Advanced.HeartbeatInterval = TimeSpan.Zero;
        Validate(options).Succeeded.ShouldBeTrue();
    }

    [Test]
    public void Sink_retry_base_delay_must_be_positive()
    {
        var options = ValidOptions();
        options.SinkRetry.BaseDelay = TimeSpan.Zero;

        Validate(options).Failed.ShouldBeTrue();
    }

    [Test]
    public void Sink_retry_max_delay_must_be_at_least_base_delay()
    {
        var options = ValidOptions();
        options.SinkRetry.BaseDelay = TimeSpan.FromSeconds(10);
        options.SinkRetry.MaxDelay = TimeSpan.FromSeconds(5);

        Validate(options).Failed.ShouldBeTrue();
    }
}
