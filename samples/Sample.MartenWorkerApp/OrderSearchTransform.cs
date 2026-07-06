using Marten;
using Wallaby.Abstractions;
using Wallaby.Providers.Marten;

namespace Sample.MartenWorkerApp;

/// <summary>Flattens each order and enriches it with the customer's name. Resolved from the container.</summary>
public sealed class OrderSearchTransform : IWallabyMartenTransform<Order>
{
    public async Task<IReadOnlyDictionary<DocumentKey, WallabyDocument?>> TransformAsync(
        IQuerySession querySession, IReadOnlyList<ChangeEvent<Order>> changes, CancellationToken ct)
    {
        var customerIds = changes.Select(c => c.Entity!.CustomerId).Distinct().ToList();
        var customers = await querySession.Query<Customer>()
            .Where(c => customerIds.Contains(c.Id))
            .ToListAsync(ct);
        var names = customers.ToDictionary(c => c.Id, c => c.Name);

        var documents = new Dictionary<DocumentKey, WallabyDocument?>(changes.Count);
        foreach (var change in changes)
        {
            var order = change.Entity!;
            documents[change.Key] = new WallabyDocument
            {
                ["number"] = order.Number,
                ["total"] = order.Total,
                ["customer"] = names.GetValueOrDefault(order.CustomerId),
            };
        }
        return documents;
    }
}
