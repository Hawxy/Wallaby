using Npgsql;
using Wallaby.Providers.Tables;
using Wallaby.TestInfrastructure.EntityFrameworkCore;
using Wallaby.TestModel;

namespace Wallaby.TestInfrastructure.Tables;

/// <summary>
/// A <see cref="PostgresFixture"/> with the plain-table test schema (<c>tables.orders</c>,
/// <c>tables.order_lines</c>, <c>tables.gizmo</c>) created by DDL, plus the EF Core test model so a
/// suite can run both providers on one slot.
/// </summary>
public sealed class TablesFixture : PostgresFixture
{
    public const string Schema = "tables";

    /// <summary>The registrations every plain-table suite shares.</summary>
    public static void Configure(TablesModelBuilder tables)
    {
        tables.UseSnakeCase().DefaultSchema(Schema);
        tables.Add<Order>();
        tables.Add<OrderLine>().HasKey(l => l.OrderId, l => l.LineNo);
        tables.Add<Gizmo>();
    }

    protected override async Task BootstrapAsync(string connectionString)
    {
        await using (var ctx = new AppDbContext(TestModelFactory.CreateOptions(connectionString)))
        {
            await ctx.Database.EnsureCreatedAsync();
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand($"""
            CREATE SCHEMA IF NOT EXISTS {Schema};
            CREATE TABLE {Schema}.orders (
                id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                customer_ref text NOT NULL,
                total numeric(12,2) NOT NULL,
                status text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                notes text);
            CREATE TABLE {Schema}.order_lines (
                order_id int NOT NULL,
                line_no int NOT NULL,
                sku text NOT NULL,
                qty int NOT NULL,
                PRIMARY KEY (order_id, line_no));
            CREATE TABLE {Schema}.gizmo (
                id uuid PRIMARY KEY,
                display_name text NOT NULL,
                unit_price numeric NOT NULL);
            ALTER TABLE {Schema}.gizmo REPLICA IDENTITY FULL;
            """, connection);
        await cmd.ExecuteNonQueryAsync();
    }
}
