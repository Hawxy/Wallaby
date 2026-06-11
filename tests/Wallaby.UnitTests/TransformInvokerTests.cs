using EFCore.CDC.TestModel;
using Wallaby.Abstractions;
using Wallaby.Internal.Pipeline;

namespace EFCore.CDC.UnitTests;

public class TransformInvokerTests
{
    [Test]
    public async Task Mismatched_entity_type_throws_instead_of_passing_a_null_entity()
    {
        var invoker = new TransformInvoker<Product>(new DelegateTransform<Product>((_, _, _) =>
            Task.FromResult<IReadOnlyDictionary<DocumentKey, CdcDocument?>>(
                new Dictionary<DocumentKey, CdcDocument?>())));

        var change = new ChangeEvent(
            ChangeAction.Update,
            new ChangeMetadata("public", "categories", null, 0, 0, IsBackfill: false),
            Entity: new Category { Name = "not-a-product" },
            Record: new Dictionary<string, object?>(),
            Changes: null,
            PrimaryKey: [1])
        {
            EntityClrType = typeof(Category),
        };

        await Assert.That(async () => _ = await invoker.InvokeAsync(db: null!, [change], CancellationToken.None))
            .Throws<InvalidOperationException>();
    }
}
