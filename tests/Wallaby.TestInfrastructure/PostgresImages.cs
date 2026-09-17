namespace Wallaby.TestInfrastructure;

/// <summary>Docker images for the Postgres containers the integration suites start.</summary>
public static class PostgresImages
{
    /// <summary>
    /// The image every version-agnostic fixture uses. Overridable through <c>WALLABY_TEST_PG_IMAGE</c>
    /// so the whole suite can run against another major (the Nuke build's Postgres matrix does this).
    /// </summary>
    public static string Default { get; } =
        Environment.GetEnvironmentVariable("WALLABY_TEST_PG_IMAGE") is { Length: > 0 } image ? image : "postgres:17";

    /// <summary>Pinned image for tests that exercise behaviour of servers before PostgreSQL 18.</summary>
    public const string Postgres17 = "postgres:17";

    /// <summary>
    /// Pinned image for tests that exercise PostgreSQL 19 behaviour specifically. Docker Hub has no
    /// <c>postgres:19</c> tag until GA; switch to it then.
    /// </summary>
    public const string Postgres19 = "postgres:19beta3";
}
