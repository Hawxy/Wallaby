namespace Wallaby.Client;

/// <summary>
/// Thrown when a control operation's <see cref="WallabySuspendOptions.Timeout"/> expires before the
/// operation completed. The underlying request stays persisted in the database.
/// </summary>
public sealed class WallabyControlTimeoutException : Exception
{
    internal WallabyControlTimeoutException(string message, WallabyControlState lastObservedState)
        : base(message)
    {
        LastObservedState = lastObservedState;
    }

    /// <summary>The control-plane state at the last poll before the timeout.</summary>
    public WallabyControlState LastObservedState { get; }
}
