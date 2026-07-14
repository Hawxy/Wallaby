using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Wallaby.Abstractions;
using Wallaby.Internal.Replication;
using Wallaby.Internal.SelfConfig;
using Wallaby.Model;
using Wallaby.Providers.Marten.Internal;
using Wallaby.TestInfrastructure;
using Wallaby.TestInfrastructure.Marten;

namespace Wallaby.Providers.Marten.Tests.Integration;

/// <summary>
/// Proves the raw-byte path at the replication boundary: a jsonb column declared
/// <c>ColumnReadMode.Utf8JsonBytes</c> decodes to <c>byte[]</c> (Npgsql's binary-mode converter hands over the
/// payload without the jsonb version byte), so the materializer can stream it without transcoding.
/// </summary>
[NotInParallel]
[ClassDataSource<MartenStoreFixture>(Shared = SharedType.PerTestSession)]
public class MartenReplicationDecoderTests(MartenStoreFixture pg)
{
    [Test]
    public async Task The_jsonb_data_column_streams_as_utf8_bytes()
    {
        await using var names = ReplicationScope.Unique(pg.ConnectionString);
        var model = new MartenModelProvider(pg.Store.Options)
            .BuildCapturePlan(new CaptureSpec { DeclaredEntities = [typeof(Widget)] }).Model;

        var configurator = new PostgresSelfConfigurator(
            pg.DataSource,
            new SelfConfigOptions { SlotName = names.Slot, PublicationName = names.Publication },
            NullLogger.Instance);
        await configurator.EnsureConfiguredAsync(model, CancellationToken.None);

        var id = Guid.NewGuid();
        await using (var session = pg.Store.LightweightSession())
        {
            session.Store(new Widget { Id = id, Name = "byte-widget", Qty = 7 });
            await session.SaveChangesAsync();
        }

        RawChange? insert = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await using var spill = new PostgresUnloggedTableSpill(pg.DataSource, names.Slot);
        await using var stream = new LogicalReplicationStream(
            pg.ConnectionString, names.Slot, names.Publication, spill, model: model);
        await foreach (var txn in stream.ReadAsync(cts.Token))
        {
            insert = txn.Changes.FirstOrDefault(
                c => c.TableName == "mt_doc_widget" && c.Action == ChangeAction.Insert);
            if (insert is not null)
            {
                break;
            }
        }

        var data = insert.ShouldNotBeNull().NewValues!.Single(c => c.ColumnName == "data");
        var bytes = data.Value.ShouldBeOfType<byte[]>();
        var json = Encoding.UTF8.GetString(bytes);
        json.ShouldStartWith("{"); // raw JSON — no jsonb version byte prefix
        json.ShouldContain("byte-widget");
    }
}
