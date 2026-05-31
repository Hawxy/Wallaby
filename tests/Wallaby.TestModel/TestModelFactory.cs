using Microsoft.EntityFrameworkCore;

namespace EFCore.CDC.TestModel;

/// <summary>Helpers for building <see cref="AppDbContext"/> instances in tests.</summary>
public static class TestModelFactory
{
    /// <summary>A placeholder connection string sufficient for model-only (no-DB) inspection.</summary>
    public const string ModelOnlyConnectionString = "Host=localhost;Database=model_only;Username=none;Password=none";

    /// <summary>Build options for the given connection string.</summary>
    public static DbContextOptions<AppDbContext> CreateOptions(string connectionString)
        => new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(connectionString)
            .Options;

    /// <summary>
    /// Create a context wired to the Npgsql provider without connecting; suitable for inspecting
    /// <see cref="DbContext.Model"/> in unit tests.
    /// </summary>
    public static AppDbContext CreateModelOnlyContext()
        => new(CreateOptions(ModelOnlyConnectionString));
}
