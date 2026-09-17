using Wallaby.Internal.SelfConfig;

namespace Wallaby.Tests.Unit;

public class ColumnListPlannerTests
{
    private static PublicationTableSpec Listed(params string[] columns)
        => new("public", "products", columns);

    private static TableCatalogInfo Catalog(
        string relReplIdent = "d", string[]? indexColumns = null, string relKind = "r")
        => new(relKind, relReplIdent, indexColumns ?? []);

    [Test]
    public void Whole_table_candidate_passes_through()
    {
        var candidate = PublicationTableSpec.WholeTable("public", "products");

        var (effective, warning) = ColumnListPlanner.Plan(candidate, Catalog("f"));

        effective.ShouldBeSameAs(candidate);
        warning.ShouldBeNull();
    }

    [Test]
    public void Missing_catalog_info_passes_through()
    {
        // Model ahead of migrations: the table isn't in pg_class yet; the DDL fails fast instead.
        var candidate = Listed("id", "name");

        var (effective, warning) = ColumnListPlanner.Plan(candidate, catalog: null);

        effective.ShouldBeSameAs(candidate);
        warning.ShouldBeNull();
    }

    [Test]
    public void Partitioned_table_demotes_to_whole_table_with_warning()
    {
        // Leaf replica identities aren't visible to the root-level catalog pass; a list missing a FULL
        // leaf's identity errors the application's own UPDATE/DELETE.
        var (effective, warning) = ColumnListPlanner.Plan(Listed("id", "name"), Catalog(relKind: "p"));

        effective.Columns.ShouldBeNull();
        warning.ShouldNotBeNull();
        warning!.ShouldContain("partitioned");
        warning.ShouldContain("public.products");
    }

    [Test]
    public void Replica_identity_full_demotes_to_whole_table_with_warning()
    {
        var (effective, warning) = ColumnListPlanner.Plan(Listed("id", "name"), Catalog("f"));

        effective.Columns.ShouldBeNull();
        warning.ShouldNotBeNull();
        warning!.ShouldContain("REPLICA IDENTITY FULL");
        warning.ShouldContain("public.products");
    }

    [Test]
    public void Replica_identity_index_covered_keeps_list()
    {
        var (effective, warning) = ColumnListPlanner.Plan(
            Listed("id", "tenant_id", "name"), Catalog("i", indexColumns: ["tenant_id", "id"]));

        effective.Columns.ShouldBe(["id", "tenant_id", "name"]);
        warning.ShouldBeNull();
    }

    [Test]
    public void Replica_identity_index_uncovered_demotes_with_warning()
    {
        var (effective, warning) = ColumnListPlanner.Plan(
            Listed("id", "name"), Catalog("i", indexColumns: ["external_ref"]));

        effective.Columns.ShouldBeNull();
        warning.ShouldNotBeNull();
        warning!.ShouldContain("external_ref");
    }

    [Test]
    public void Replica_identity_nothing_keeps_list()
    {
        // 'n' already breaks published UPDATE/DELETE at DML time; a column list adds no new failure mode.
        var (effective, warning) = ColumnListPlanner.Plan(Listed("id", "name"), Catalog("n"));

        effective.Columns.ShouldBe(["id", "name"]);
        warning.ShouldBeNull();
    }
}
