using Npgsql;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;

namespace Wallaby.Tests.Unit;

/// <summary>
/// The reselector's unhealable guards: a change whose primary key was itself not carried (absent,
/// null, or unchanged TOAST) cannot be re-read and must throw before any database access.
/// </summary>
public class RowReselectorTests
{
    private static WallabyModel ProductModel()
    {
        var id = new CapturedColumn
        {
            PropertyName = "Id", ColumnName = "Id", ClrType = typeof(int), IsPrimaryKey = true,
        };
        var description = new CapturedColumn
        {
            PropertyName = "Description", ColumnName = "Description", ClrType = typeof(string), IsPrimaryKey = false,
        };
        return new WallabyModel([
            new CapturedTable
            {
                EntityClrType = typeof(object),
                Schema = "public",
                TableName = "products",
                Columns = [id, description],
                PrimaryKey = [id],
            },
        ]);
    }

    // The data source is never opened: the guard throws first.
    private static RowReselector Reselector()
        => new(NpgsqlDataSource.Create("Host=localhost;Database=never_opened;Username=u;Password=p"), ProductModel());

    private static RawChange Change(params RawColumn[] newValues) => new()
    {
        RelationId = 1,
        Schema = "public",
        TableName = "products",
        Action = ChangeAction.Update,
        NewValues = newValues,
    };

    [Test]
    public async Task A_toasted_primary_key_is_unhealable()
    {
        var change = Change(
            new RawColumn { ColumnName = "Id", IsUnchangedToast = true },
            new RawColumn { ColumnName = "Description", IsUnchangedToast = true });

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await Reselector().ReselectAsync(change, CancellationToken.None));

        ex.Message.ShouldContain("primary key column 'Id'");
    }

    [Test]
    public async Task An_absent_primary_key_is_unhealable()
    {
        var change = Change(new RawColumn { ColumnName = "Description", IsUnchangedToast = true });

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await Reselector().ReselectAsync(change, CancellationToken.None));

        ex.Message.ShouldContain("primary key column 'Id'");
    }

    [Test]
    public async Task An_uncaptured_table_is_unhealable()
    {
        var change = Change(new RawColumn { ColumnName = "Id", Value = 1 }) with { TableName = "not_mapped" };

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await Reselector().ReselectAsync(change, CancellationToken.None));

        ex.Message.ShouldContain("not part of the model");
    }
}
