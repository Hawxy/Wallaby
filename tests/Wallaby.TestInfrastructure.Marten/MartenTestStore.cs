using Marten;

namespace Wallaby.TestInfrastructure.Marten;

/// <summary>Builds the shared Marten test store over the test document types.</summary>
public static class MartenTestStore
{
    /// <summary>The schema the test documents live in (distinct from <c>public</c> to prove qualification).</summary>
    public const string Schema = "docs";

    public static StoreOptions CreateOptions(string connectionString)
    {
        var options = new StoreOptions();
        options.Connection(connectionString);
        options.DatabaseSchemaName = Schema;
        options.RegisterDocumentType<Widget>();
        options.Schema.For<SoftWidget>().SoftDeleted();
        options.Schema.For<TenantWidget>().MultiTenanted();
        return options;
    }

    public static DocumentStore Create(string connectionString) => new(CreateOptions(connectionString));
}
