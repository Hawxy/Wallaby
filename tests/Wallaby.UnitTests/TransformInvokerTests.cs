using Wallaby.Abstractions;
using Wallaby.EntityFrameworkCore.Internal;
using Wallaby.EntityFrameworkCore;
using Wallaby.Internal.Pipeline;
using Wallaby.TestModel;

namespace Wallaby.UnitTests;

public class TransformInvokerTests
{
    [Test]
    public async Task Mismatched_entity_type_throws_instead_of_passing_a_null_entity()
    {
        var invoker = new EfCoreTransformInvoker<Product>(new DelegateTransform<Product>((_, _, _) =>
            Task.FromResult<IReadOnlyDictionary<DocumentKey, WallabyDocument?>>(
                new Dictionary<DocumentKey, WallabyDocument?>())));

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

        await Should.ThrowAsync<InvalidOperationException>(
            async () => _ = await invoker.InvokeAsync(session: null!, [change], CancellationToken.None));
    }
}
