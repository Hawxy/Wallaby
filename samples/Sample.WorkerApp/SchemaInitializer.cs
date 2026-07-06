using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace Sample.WorkerApp;

/// <summary>Ensures the sample tables exist before CDC starts (demo convenience only).</summary>
internal sealed class SchemaInitializer(IDbContextFactory<SampleDbContext> factory) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var context = await factory.CreateDbContextAsync(cancellationToken);
        // EnsureCreated skips table creation when the database already has any (e.g. the Marten
        // sample's), so create this context's tables directly and ignore "already exists".
        var creator = context.GetService<IRelationalDatabaseCreator>();
        if (!await creator.ExistsAsync(cancellationToken))
            await creator.CreateAsync(cancellationToken);
        try
        {
            await creator.CreateTablesAsync(cancellationToken);
        }
        catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.DuplicateTable)
        {
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
