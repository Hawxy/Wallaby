namespace Wallaby.Sinks;

/// <summary>
/// One item of a <c>_bulk</c> response, as classified by
/// <see cref="BulkJson.ClassifyItems(IEnumerable{BulkItemResult}, string)"/>.
/// </summary>
/// <param name="Action">The bulk action name (<c>index</c>, <c>delete</c>, ...).</param>
/// <param name="Status">The item's HTTP status.</param>
/// <param name="Id">The document id, used in failure messages.</param>
/// <param name="Error">The item's error as <c>type: reason</c>, or null when it has none.</param>
public readonly record struct BulkItemResult(string Action, int Status, string? Id, string? Error);
