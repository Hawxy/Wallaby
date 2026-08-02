using System.Diagnostics.CodeAnalysis;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;
using Wallaby.Model;
using Wallaby.Providers;

namespace Wallaby.Tests.Unit;

public class ChangeEventFactoryTests
{
    private sealed class ThrowingMaterializer : IRowMaterializer
    {
        public bool TryMaterialize(RawChange change, [NotNullWhen(true)] out MaterializedRow? row)
            => throw new FormatException("bad enum value 'Blue'");
    }

    [Test]
    public async Task Materialization_failure_is_annotated_with_table_and_commit_position()
    {
        var factory = new ChangeEventFactory(new ThrowingMaterializer());
        var change = new RawChange
        {
            RelationId = 1,
            Schema = "public",
            TableName = "products",
            Action = ChangeAction.Update,
            CommitLsn = 42,
            CommitIdx = 7,
        };

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await factory.CreateAsync(change, CancellationToken.None));

        ex.Message.ShouldContain("public.products");
        ex.Message.ShouldContain("Update");
        ex.Message.ShouldContain("0/2A");
        ex.Message.ShouldContain("change #7");
        ex.Message.ShouldContain("bad enum value 'Blue'");
        ex.InnerException.ShouldBeOfType<FormatException>();
    }
}
