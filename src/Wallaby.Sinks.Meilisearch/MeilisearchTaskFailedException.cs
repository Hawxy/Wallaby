using Meilisearch;

namespace Wallaby.Sinks.Meilisearch;

/// <summary>
/// Thrown when a Meilisearch indexing task finishes <see cref="TaskInfoStatus.Failed"/> or
/// <see cref="TaskInfoStatus.Canceled"/>. <see cref="Code"/> carries the task's Meilisearch error code
/// (when it reported one) and drives the sink's retryable-vs-permanent classification.
/// </summary>
public sealed class MeilisearchTaskFailedException(int taskUid, TaskInfoStatus status, string? code, string detail)
    : Exception($"Meilisearch task {taskUid} finished with status {status}: {detail}")
{
    /// <summary>The failed task's uid.</summary>
    public int TaskUid { get; } = taskUid;

    /// <summary>The task's terminal status (<c>Failed</c> or <c>Canceled</c>).</summary>
    public TaskInfoStatus Status { get; } = status;

    /// <summary>Meilisearch error code (e.g. <c>invalid_document_fields</c>), or null when the task carried no detail.</summary>
    public string? Code { get; } = code;
}
